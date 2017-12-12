#ifndef STATS_H
#define STATS_H

#include <sys/types.h>
#include "socket.h"
#include "slot.h"

struct memory_stats {
    long long buffers;          // 表示正在使用的缓冲区的数量
    long long cmds;             // 表示正在使用的command对象的数量
    long long conns;
    long long conn_info;        // 表示正在使用的conn_info的数量
    long long buf_times;        // 表示正在使用的buf_time的数量

    long long free_buffers;     // 表示空闲的缓冲区数量, 大小与context的free_mbufq队列长度保持一致
    long long free_cmds;        // 表示空闲的command对象的数量, 大小与context的free_cmdq队列长度保持一致
    long long free_conns;
    long long free_conn_info;   // 表示空闲的conn_info对象的数量, 大小与context的free_conn_infoq队列长度保持一致
    long long free_buf_times;   // 表示空闲的buf_time对象的数量, 大小与context的free_buf_timeq队列长度保持一致
};

struct basic_stats {
    long long connected_clients;    // 客户端与corvus建立连接的数量
    long long completed_commands;   // 成功发送请求并获得相应的请求数量
    long long slot_update_jobs;     // 触发slot信息更新的次数
    long long recv_bytes;           // 收到的字节大小(包括从客户端->corvus的数据, 和redis实例->corvus的数据)
    long long send_bytes;           // 发送的字节大小(包括从corvus->客户端的数据, 和corvus->redis实例的数据)

    long long remote_latency;
    long long total_latency;

    long long ask_recv;
    long long moved_recv;
};

struct stats {
    double used_cpu_sys;
    double used_cpu_user;

    long long last_command_latency[MAX_NODE_LIST];
    char remote_nodes[MAX_NODE_LIST * ADDRESS_LEN];

    struct basic_stats basic;
};

int stats_init();
void stats_kill();
int stats_resolve_addr(char *addr);
void stats_get(struct stats *stats);
void stats_get_memory(struct memory_stats *stats);

void incr_slot_update_counter();

#endif /* end of include guard: STATS_H */
