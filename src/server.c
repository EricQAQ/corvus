#include <stdlib.h>
#include "server.h"
#include "corvus.h"
#include "event.h"
#include "logging.h"
#include "socket.h"
#include "slot.h"

static const char *req_ask = "*1\r\n$6\r\nASKING\r\n";

static inline void remove_queue_head(struct connection *server,
        struct command *cmd, int retry)
{
    if (retry) {
        STAILQ_REMOVE_HEAD(&server->retry_queue, retry_next);
        STAILQ_NEXT(cmd, retry_next) = NULL;
    } else {
        STAILQ_REMOVE_HEAD(&server->ready_queue, ready_next);
        STAILQ_NEXT(cmd, ready_next) = NULL;
    }
}

static void server_free_buf(struct command *cmd)
{
    struct mbuf *b, *buf;
    b = cmd->rep_buf[0].buf;
    while (b != NULL) {
        if (b == cmd->rep_buf[0].buf && b == cmd->rep_buf[1].buf) {
            mbuf_dec_ref_by(b, 2);
        } else if (b == cmd->rep_buf[0].buf || b == cmd->rep_buf[1].buf) {
            mbuf_dec_ref(b);
        }

        buf = STAILQ_NEXT(b, next);
        if (b->refcount <= 0 && (b != cmd->rep_buf[1].buf
                    || cmd->rep_buf[1].pos >= b->last))
        {
            STAILQ_REMOVE(&cmd->server->data, b, mbuf, next);
            STAILQ_NEXT(b, next) = NULL;
            mbuf_recycle(cmd->ctx, b);
        }
        if (b == cmd->rep_buf[1].buf) break;
        b = buf;
    }
    memset(&cmd->rep_buf, 0, sizeof(cmd->rep_buf));
}

static int server_write(struct connection *server, int retry)
{
    int status;
    struct command *cmd;

    cmd = retry ? STAILQ_FIRST(&server->retry_queue) : STAILQ_FIRST(&server->ready_queue);
    if (cmd->stale) {
        remove_queue_head(server, cmd, retry);
        cmd_free(cmd);
        return CORVUS_OK;
    }

    if (cmd->iov.head == NULL) {
        if (cmd->asking) {
            cmd_iov_add(&cmd->iov, (void*)req_ask, strlen(req_ask));
        }
        cmd_create_iovec(&cmd->req_buf[0], &cmd->req_buf[1], &cmd->iov);
        cmd->iov.head = cmd->iov.data;
        cmd->iov.size = cmd->iov.len;
    }

    if (cmd->iov.len <= 0) {
        LOG(WARN, "no data to write");
        return CORVUS_ERR;
    }

    status = cmd_write_iov(cmd, server->fd);

    if (status == CORVUS_ERR) return CORVUS_ERR;
    if (status == CORVUS_AGAIN) return CORVUS_OK;

    if (cmd->iov.len <= 0) {
        cmd_free_iov(&cmd->iov);
        remove_queue_head(server, cmd, retry);
        STAILQ_INSERT_TAIL(&server->waiting_queue, cmd, waiting_next);
    } else {
        if (conn_register(server) == CORVUS_ERR) {
            LOG(ERROR, "fail to reregister server %d", server->fd);
            return CORVUS_ERR;
        }
    }
    return CORVUS_OK;
}

static int server_redirect(struct command *cmd, struct redirect_info *info)
{
    int port;
    struct address addr;

    server_free_buf(cmd);
    redis_data_destroy(cmd->rep_data);
    cmd->rep_data = NULL;

    port = socket_parse_addr(info->addr, &addr);
    if (port == CORVUS_ERR) {
        LOG(WARN, "server_redirect: fail to parse addr %s", info->addr);
        cmd_mark_fail(cmd);
        return CORVUS_OK;
    }

    struct connection *server = conn_get_server_from_pool(cmd->ctx, &addr);
    if (server == NULL) {
        LOG(WARN, "server_redirect: fail to get server %s", info->addr);
        cmd_mark_fail(cmd);
        return CORVUS_OK;
    }

    cmd->server = server;
    STAILQ_INSERT_TAIL(&server->retry_queue, cmd, retry_next);

    switch (conn_register(server)) {
        case CORVUS_ERR: return CORVUS_ERR;
        case CORVUS_OK: break;
    }
    return CORVUS_OK;
}

static int read_one_reply(struct connection *server)
{
    if (STAILQ_EMPTY(&server->waiting_queue)) return CORVUS_AGAIN;

    struct command *cmd = STAILQ_FIRST(&server->waiting_queue);

    int status = cmd_read_reply(cmd, server);
    if (status == CORVUS_OK) {
        if (!cmd->asking) {
            STAILQ_REMOVE_HEAD(&server->waiting_queue, waiting_next);
            STAILQ_NEXT(cmd, waiting_next) = NULL;
        } else {
            LOG(DEBUG, "recv asking");
            server_free_buf(cmd);
            redis_data_destroy(cmd->rep_data);
            cmd->rep_data = NULL;
            cmd->asking = 0;
            return CORVUS_OK;
        }
    }

    switch (status) {
        case CORVUS_AGAIN: return CORVUS_AGAIN;
        case CORVUS_EOF: return CORVUS_EOF;
        case CORVUS_ERR: return CORVUS_ERR;
    }

    if (cmd->stale) {
        server_free_buf(cmd);
        cmd_free(cmd);
        return CORVUS_OK;
    }

    if (cmd->rep_data->type != REP_ERROR) {
        cmd_mark_done(cmd);
        return CORVUS_OK;
    }

    struct redirect_info info = {.addr = NULL, .type = CMD_ERR, .slot = -1};
    cmd_parse_redirect(cmd, &info);
    switch (info.type) {
        case CMD_ERR_MOVED:
            if (server_redirect(cmd, &info) == CORVUS_ERR) {
                if (info.addr != NULL) free(info.addr);
                return CORVUS_ERR;
            }
            slot_create_job(SLOT_UPDATE, NULL);
            break;
        case CMD_ERR_ASK:
            if (server_redirect(cmd, &info) == CORVUS_ERR) {
                if (info.addr != NULL) free(info.addr);
                return CORVUS_ERR;
            }
            cmd->asking = 1;
            break;
        default:
            cmd_mark_done(cmd);
            break;
    }
    if (info.addr != NULL) free(info.addr);
    return CORVUS_OK;
}

static int server_read(struct connection *server)
{
    int status;
    do {
        status = read_one_reply(server);
    } while (status == CORVUS_OK);
    return status;
}

static void server_ready(struct connection *self, uint32_t mask)
{
    int retry = -1;

    if (mask & E_ERROR) {
        LOG(DEBUG, "error");
        server_eof(self);
        return;
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "server writable");
        if (self->status == CONNECTING) self->status = CONNECTED;
        if (self->status == CONNECTED) {
            if (!STAILQ_EMPTY(&self->retry_queue)) {
                retry = 1;
            } else if (!STAILQ_EMPTY(&self->ready_queue)) {
                retry = 0;
            }

            if (retry != -1 && server_write(self, retry) == CORVUS_ERR) {
                server_eof(self);
                return;
            }
        } else {
            LOG(ERROR, "server not connected");
            server_eof(self);
            return;
        }
    }
    if (mask & E_READABLE) {
        LOG(DEBUG, "server readable");

        if (!STAILQ_EMPTY(&self->waiting_queue)) {
            switch (server_read(self)) {
                case CORVUS_ERR:
                case CORVUS_EOF:
                    server_eof(self);
                    return;
            }
        } else {
            LOG(WARN, "server is readable but waiting_queue is empty");
            server_eof(self);
            return;
        }
    }
}

struct connection *server_create(struct context *ctx, int fd)
{
    struct connection *server = conn_create(ctx);
    server->fd = fd;
    server->ready = server_ready;
    return server;
}

void server_eof(struct connection *server)
{
    LOG(WARN, "server eof");

    struct command *c;
    while (!STAILQ_EMPTY(&server->ready_queue)) {
        c = STAILQ_FIRST(&server->ready_queue);
        STAILQ_REMOVE_HEAD(&server->ready_queue, ready_next);
        STAILQ_NEXT(c, ready_next) = NULL;
        cmd_free_iov(&c->iov);
        cmd_mark_fail(c);
    }

    while (!STAILQ_EMPTY(&server->waiting_queue)) {
        c = STAILQ_FIRST(&server->waiting_queue);
        STAILQ_REMOVE_HEAD(&server->waiting_queue, waiting_next);
        STAILQ_NEXT(c, waiting_next) = NULL;
        cmd_free_iov(&c->iov);
        cmd_mark_fail(c);
    }

    while (!STAILQ_EMPTY(&server->retry_queue)) {
        c = STAILQ_FIRST(&server->retry_queue);
        STAILQ_REMOVE_HEAD(&server->retry_queue, retry_next);
        STAILQ_NEXT(c, retry_next) = NULL;
        cmd_free_iov(&c->iov);
        cmd_mark_fail(c);
    }

    event_deregister(server->ctx->loop, server);
    conn_free(server);
    slot_create_job(SLOT_UPDATE, NULL);
}
