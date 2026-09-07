#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/sem.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <pthread.h>
#include <sys/select.h>
#include <errno.h>
#include "ksocket.h"

// External shared resources
extern SHM* SM;
extern SOCKET_info* socket_info;
extern struct sembuf pop, vop;
extern int sem1, sem2;
extern int sem_SM;
extern int sem_socket_info;
extern int shmid_SM;
extern int shmid_socket_info;

int total_transmissions = 0;

void *thread_R(void* arg) {
    printf("Thread R started.\n");
    fd_set read_fds;
    FD_ZERO(&read_fds);
    int max_fd = 0;

    while(1) {
        struct timeval timeout = { .tv_sec = T, .tv_usec = 0 };
        fd_set curr_fds = read_fds;
        int ready = select(max_fd + 1, &curr_fds, NULL, NULL, &timeout);
        if (ready == -1) {
            perror("select error");
        }
        if (ready <= 0) {
            // Timeout: reset and update file descriptors
            FD_ZERO(&read_fds);
            max_fd = 0;
            waitSem(sem_SM);
            for (int i = 0; i < N; i++) {
                if (!SM[i].free) {
                    FD_SET(SM[i].udp_socket_id, &read_fds);
                    if (SM[i].udp_socket_id > max_fd)
                        max_fd = SM[i].udp_socket_id;
                    if (SM[i].nospace && SM[i].rwnd.size > 0) {
                        SM[i].nospace = 0;
                        int last_recv_seq = (SM[i].rwnd.base - 1 + MAX_SEQ_NUM) % MAX_SEQ_NUM;
                        struct sockaddr_in client_addr;
                        client_addr.sin_family = AF_INET;
                        client_addr.sin_port = htons(SM[i].remote_port);
                        client_addr.sin_addr.s_addr = inet_addr(SM[i].remote_ip);
                        char ack[13];
                        ack[0] = '0';
                        for (int j = 1; j <= 8; j++)
                            ack[j] = (last_recv_seq >> (8 - j)) % 2 + '0';
                        for (int j = 9; j <= 12; j++)
                            ack[j] = (SM[i].rwnd.size >> (12 - j)) % 2 + '0';
                        printf("Buffer space available. Sending ACK with last_recv_seq = %d, rwnd = %d\n", last_recv_seq, SM[i].rwnd.size);
                        sendto(SM[i].udp_socket_id, ack, 13, 0, (struct sockaddr*)&client_addr, sizeof(client_addr));
                    }
                }
            }
            signalSem(sem_SM);
        } else {
            waitSem(sem_SM);
            for (int i = 0; i < N; i++) {
                if (!SM[i].free && FD_ISSET(SM[i].udp_socket_id, &curr_fds)) {
                    char buffer[MAX_MESG_LENGTH + 19];
                    struct sockaddr_in client_addr;
                    socklen_t addr_len = sizeof(client_addr);
                    int n = recvfrom(SM[i].udp_socket_id, buffer, sizeof(buffer), 0, (struct sockaddr*)&client_addr, &addr_len);
                    if (dropMessage(P)) {
                        int seq = (buffer[1]-'0')*128 + (buffer[2]-'0')*64 + (buffer[3]-'0')*32 +
                                  (buffer[4]-'0')*16 + (buffer[5]-'0')*8 + (buffer[6]-'0')*4 +
                                  (buffer[7]-'0')*2 + (buffer[8]-'0');
                        if (buffer[0] == '1')
                            printf("Simulated drop: Data packet seq %d\n", seq);
                        else
                            printf("Simulated drop: ACK for seq %d\n", seq);
                        continue;
                    }
                    if (n == -1) {
                        perror("recvfrom error");
                        exit(1);
                    } else {
                        if (buffer[0] == '$') {
                            printf("Special query received.\n");
                            int last_recv_seq = (SM[i].rwnd.base - 1 + MAX_SEQ_NUM) % MAX_SEQ_NUM;
                            struct sockaddr_in client_addr;
                            client_addr.sin_family = AF_INET;
                            client_addr.sin_port = htons(SM[i].remote_port);
                            client_addr.sin_addr.s_addr = inet_addr(SM[i].remote_ip);
                            char ack[13];
                            ack[0] = '0';
                            for (int j = 1; j <= 8; j++)
                                ack[j] = (last_recv_seq >> (8 - j)) % 2 + '0';
                            for (int j = 9; j <= 12; j++)
                                ack[j] = (SM[i].rwnd.size >> (12 - j)) % 2 + '0';
                            sendto(SM[i].udp_socket_id, ack, 13, 0, (struct sockaddr*)&client_addr, sizeof(client_addr));
                        } else if (buffer[0] == '0') {
                            int seq = (buffer[1]-'0')*128 + (buffer[2]-'0')*64 + (buffer[3]-'0')*32 +
                                      (buffer[4]-'0')*16 + (buffer[5]-'0')*8 + (buffer[6]-'0')*4 +
                                      (buffer[7]-'0')*2 + (buffer[8]-'0');
                            int new_rwnd = (buffer[9]-'0')*8 + (buffer[10]-'0')*4 +
                                           (buffer[11]-'0')*2 + (buffer[12]-'0');
                            printf("Received ACK for seq %d\n", seq);
                            if (SM[i].swnd.arr[seq] >= 0) {
                                int pos = SM[i].swnd.base;
                                while (pos != (seq + 1) % MAX_SEQ_NUM) {
                                    SM[i].swnd.arr[pos] = -1;
                                    SM[i].last_sent_time[pos] = -1;
                                    SM[i].send_buffer_size++;
                                    pos = (pos + 1) % MAX_SEQ_NUM;
                                }
                                SM[i].swnd.base = (seq + 1) % MAX_SEQ_NUM;
                            }
                            SM[i].swnd.size = new_rwnd;
                        } else {
                            int seq = (buffer[1]-'0')*128 + (buffer[2]-'0')*64 + (buffer[3]-'0')*32 +
                                      (buffer[4]-'0')*16 + (buffer[5]-'0')*8 + (buffer[6]-'0')*4 +
                                      (buffer[7]-'0')*2 + (buffer[8]-'0');
                            int msg_len = (buffer[9]-'0')*512 + (buffer[10]-'0')*256 + (buffer[11]-'0')*128 +
                                          (buffer[12]-'0')*64 + (buffer[13]-'0')*32 + (buffer[14]-'0')*16 +
                                          (buffer[15]-'0')*8 + (buffer[16]-'0')*4 + (buffer[17]-'0')*2 +
                                          (buffer[18]-'0');
                            printf("Data received: seq = %d, len = %d\n", seq, msg_len);
                            int send_ack = 0;
                            if (seq == SM[i].rwnd.base) {
                                int buf_idx = SM[i].rwnd.arr[seq];
                                memcpy(SM[i].recv_buffer[buf_idx], buffer + 19, msg_len);
                                printf("Stored message: seq = %d\n", seq);
                                SM[i].recv_buffer_valid[buf_idx] = 1;
                                SM[i].rwnd.size--;
                                SM[i].len_recv_buffer[buf_idx] = msg_len;
                                while(SM[i].rwnd.arr[SM[i].rwnd.base] >= 0 &&
                                      SM[i].recv_buffer_valid[SM[i].rwnd.arr[SM[i].rwnd.base]])
                                    SM[i].rwnd.base = (SM[i].rwnd.base + 1) % MAX_SEQ_NUM;
                                send_ack = 1;
                            } else {
                                if (SM[i].rwnd.arr[seq] >= 0 &&
                                    SM[i].recv_buffer_valid[SM[i].rwnd.arr[seq]] == 0) {
                                    int buf_idx = SM[i].rwnd.arr[seq];
                                    memcpy(SM[i].recv_buffer[buf_idx], buffer + 19, msg_len);
                                    printf("Stored out-of-order message: seq = %d\n", seq);
                                    SM[i].recv_buffer_valid[buf_idx] = 1;
                                    SM[i].len_recv_buffer[buf_idx] = msg_len;
                                    SM[i].rwnd.size--;
                                }
                            }
                            printf("Sending ACK with seq = %d\n", SM[i].rwnd.base);
                            if (SM[i].rwnd.size == 0)
                                SM[i].nospace = 1;
                            seq = (SM[i].rwnd.base - 1 + MAX_SEQ_NUM) % MAX_SEQ_NUM;
                            char ack[13];
                            ack[0] = '0';
                            for (int j = 1; j <= 8; j++)
                                ack[j] = (seq >> (8 - j)) % 2 + '0';
                            for (int j = 9; j <= 12; j++)
                                ack[j] = (SM[i].rwnd.size >> (12 - j)) % 2 + '0';
                            sendto(SM[i].udp_socket_id, ack, 13, 0, (struct sockaddr*)&client_addr, sizeof(client_addr));
                            printf("ACK sent.\n");
                        }
                    }
                }
            }
            signalSem(sem_SM);
        }
    }
    return NULL;
}

void *thread_S(void* arg) {
    printf("Thread S started.\n");
    while (1) {
        sleep(T / 2);
        waitSem(sem_SM);
        for (int i = 0; i < N; i++) {
            if (!SM[i].free) {
                struct sockaddr_in server_addr;
                server_addr.sin_family = AF_INET;
                server_addr.sin_port = htons(SM[i].remote_port);
                server_addr.sin_addr.s_addr = inet_addr(SM[i].remote_ip);

                int timeout_flag = 0;
                int base = SM[i].swnd.base;
                while (base != (SM[i].swnd.base + SM[i].swnd.size) % MAX_SEQ_NUM) {
                    if (SM[i].last_sent_time[base] != -1 &&
                        time(NULL) - SM[i].last_sent_time[base] > T) {
                        timeout_flag = 1;
                        break;
                    }
                    base = (base + 1) % MAX_SEQ_NUM;
                }

                if (timeout_flag) {
                    if (SM[i].swnd.size == 0) {
                        char special = '$';
                        printf("Sending special query.\n");
                        sendto(SM[i].udp_socket_id, &special, 1, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
                        continue;
                    }
                    base = SM[i].swnd.base;
                    int start = base;
                    while (base != (start + SM[i].swnd.size) % MAX_SEQ_NUM) {
                        if (SM[i].swnd.arr[base] != -1) {
                            int win_idx = SM[i].swnd.arr[base];
                            char buffer[MAX_MESG_LENGTH + 19];
                            buffer[0] = '1';
                            for (int j = 1; j <= 8; j++)
                                buffer[j] = (base >> (8 - j)) % 2 + '0';
                            int msg_len = SM[i].len_send_buffer[win_idx];
                            for (int j = 9; j <= 18; j++)
                                buffer[j] = (msg_len >> (18 - j)) % 2 + '0';
                            memcpy(buffer + 19, SM[i].send_buffer[win_idx], msg_len);
                            printf("Retransmitting packet: seq = %d\n", base);
                            total_transmissions++;
                            sendto(SM[i].udp_socket_id, buffer, 19 + msg_len, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
                            SM[i].last_sent_time[base] = time(NULL);
                        }
                        base = (base + 1) % MAX_SEQ_NUM;
                    }
                } else {
                    base = SM[i].swnd.base;
                    int start = base;
                    while (base != (start + SM[i].swnd.size) % MAX_SEQ_NUM) {
                        if (SM[i].swnd.arr[base] != -1 && SM[i].last_sent_time[base] == -1) {
                            int win_idx = SM[i].swnd.arr[base];
                            char buffer[MAX_MESG_LENGTH + 19];
                            buffer[0] = '1';
                            for (int j = 1; j <= 8; j++)
                                buffer[j] = (base >> (8 - j)) % 2 + '0';
                            int msg_len = SM[i].len_send_buffer[win_idx];
                            for (int j = 9; j <= 18; j++)
                                buffer[j] = (msg_len >> (18 - j)) % 2 + '0';
                            printf("Sending new packet: seq = %d, len = %d\n", base, msg_len);
                            memcpy(buffer + 19, SM[i].send_buffer[win_idx], msg_len);
                            total_transmissions++;
                            sendto(SM[i].udp_socket_id, buffer, 19 + msg_len, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
                            SM[i].last_sent_time[base] = time(NULL);
                        }
                        base = (base + 1) % MAX_SEQ_NUM;
                    }
                }
            }
        }
        signalSem(sem_SM);
    }
    return NULL;
}

void *thread_GC(void* arg) {
    while (1) {
        sleep(T);
        waitSem(sem_SM);
        for (int i = 0; i < N; i++) {
            if (!SM[i].free) {
                if (kill(SM[i].pid, 0))
                    SM[i].free = 1;
            }
            if (SM[i].free && SM[i].udp_socket_id != -1) {
                printf("Socket %d: Total transmissions = %d\n", i, total_transmissions);
                fflush(stdout);
                close(SM[i].udp_socket_id);
                SM[i].udp_socket_id = -1;
            }
        }
        signalSem(sem_SM);
    }
    return NULL;
}

void init_shared_mem() {
    int key_sm = ftok("ksocket.h", 'a');
    int key_sockinfo = ftok("ksocket.h", 'b');
    int key_sem1 = ftok("ksocket.h", 'c');
    int key_sem2 = ftok("ksocket.h", 'd');
    int key_sem_sm = ftok("ksocket.h", 'e');
    int key_sem_sockinfo = ftok("ksocket.h", 'f');

    shmid_SM = shmget(key_sm, sizeof(SHM) * N, 0777 | IPC_CREAT);
    if (shmid_SM == -1) {
        perror("Shared memory SM error");
        exit(1);
    }
    SM = (SHM*)shmat(shmid_SM, NULL, 0);
    if (SM == (void*)-1) {
        perror("Attach SM error");
        exit(1);
    }

    shmid_socket_info = shmget(key_sockinfo, sizeof(SOCKET_info), 0777 | IPC_CREAT);
    if (shmid_socket_info == -1) {
        perror("Shared memory socket_info error");
        exit(1);
    }
    socket_info = (SOCKET_info*)shmat(shmid_socket_info, NULL, 0);
    if (socket_info == (void*)-1) {
        perror("Attach socket_info error");
        exit(1);
    }

    sem1 = semget(key_sem1, 1, 0777 | IPC_CREAT);
    sem2 = semget(key_sem2, 1, 0777 | IPC_CREAT);
    sem_SM = semget(key_sem_sm, 1, 0777 | IPC_CREAT);
    sem_socket_info = semget(key_sem_sockinfo, 1, 0777 | IPC_CREAT);
    if (sem1 == -1 || sem2 == -1 || sem_SM == -1 || sem_socket_info == -1) {
        perror("Semaphore error");
        exit(1);
    }

    socket_info->sock_id = 0;
    socket_info->err_no = 0;
    strcpy(socket_info->ip_addr, "\0");
    socket_info->port = 0;

    for (int i = 0; i < N; i++) {
        SM[i].free = 1;
    }

    semctl(sem1, 0, SETVAL, 0);
    semctl(sem2, 0, SETVAL, 0);
    semctl(sem_SM, 0, SETVAL, 1);
    semctl(sem_socket_info, 0, SETVAL, 1);

    pop.sem_num = pop.sem_flg = vop.sem_num = vop.sem_flg = 0;
    pop.sem_op = -1;
    vop.sem_op = 1;
}

void cleanup_handler(int sig) {
    printf("Cleaning up and exiting...\n");
    shmdt(SM);
    shmdt(socket_info);
    shmctl(shmid_SM, IPC_RMID, NULL);
    shmctl(shmid_socket_info, IPC_RMID, NULL);
    semctl(sem1, 0, IPC_RMID);
    semctl(sem2, 0, IPC_RMID);
    semctl(sem_SM, 0, IPC_RMID);
    semctl(sem_socket_info, 0, IPC_RMID);
    exit(0);
}

int main() {
    srand(time(0));
    signal(SIGINT, cleanup_handler);

    init_shared_mem();

    pthread_t tid_S, tid_R, tid_GC;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&tid_S, &attr, thread_S, NULL);
    pthread_create(&tid_R, &attr, thread_R, NULL);
    pthread_create(&tid_GC, &attr, thread_GC, NULL);

    while (1) {
        waitSem(sem1);
        waitSem(sem_socket_info);
        if (socket_info->sock_id == 0 && socket_info->ip_addr[0] == '\0' && socket_info->port == 0) {
            int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
            if (sock_fd == -1) {
                socket_info->err_no = errno;
                socket_info->sock_id = -1;
            } else {
                socket_info->sock_id = sock_fd;
            }
        } else {
            struct sockaddr_in serv_addr;
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(socket_info->port);
            serv_addr.sin_addr.s_addr = inet_addr(socket_info->ip_addr);
            if (bind(socket_info->sock_id, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
                socket_info->err_no = errno;
                socket_info->sock_id = -1;
            }
        }
        signalSem(sem_socket_info);
        signalSem(sem2);
    }
    return 0;
}
