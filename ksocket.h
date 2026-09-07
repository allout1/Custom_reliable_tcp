#ifndef KSOCKET_H
#define KSOCKET_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/sem.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/time.h>
#include <errno.h>
#include <signal.h>

// KTP socket type
#define SOCK_KTP 1000

// KTP socket parameters
#define T 5
#define P 0.5
#define N 20

#define MAX_MESG_LENGTH 512
#define MAX_SEQ_NUM 256
#define MAX_WINDOW_SIZE 10

// Error codes
#define ENOSPACE 1001
#define ENOTBOUND 1002
#define ENOMESSAGE 1003

// Semaphore macros
#define waitSem(s) semop(s, &pop, 1)
#define signalSem(s) semop(s, &vop, 1)

// Window structure for tracking packets
typedef struct {
    int base;
    int arr[MAX_SEQ_NUM];
    int size;
} window;

// Shared memory structure for a KTP socket
typedef struct {
    int free;
    pid_t pid;
    int udp_socket_id;
    char remote_ip[16];
    uint16_t remote_port;
    char send_buffer[MAX_WINDOW_SIZE][MAX_MESG_LENGTH];
    char recv_buffer[MAX_WINDOW_SIZE][MAX_MESG_LENGTH];
    int send_buffer_size;
    int recv_buffer_size;
    int len_send_buffer[MAX_WINDOW_SIZE];
    int len_recv_buffer[MAX_WINDOW_SIZE];
    int recv_seq_num;
    int recv_buffer_valid[MAX_WINDOW_SIZE];
    window swnd;
    window rwnd;
    int nospace;
    time_t last_sent_time[MAX_WINDOW_SIZE];
} SHM;

// Structure for exchanging socket initialization info
typedef struct {
    int sock_id;
    char ip_addr[16];
    uint16_t port;
    int err_no;
} SOCKET_info;

// External variables
extern SHM* SM;
extern SOCKET_info* socket_info;
extern struct sembuf pop, vop;
extern int sem1, sem2;
extern int sem_SM;
extern int sem_socket_info;
extern int shmid_SM;
extern int shmid_socket_info;

// Function prototypes
int k_socket(int domain, int type, int protocol);
int k_bind(char src_ip[], uint16_t src_port, char dest_ip[], uint16_t dest_port);
ssize_t k_sendto(int sockfd, const void *buf, size_t len, int flags, const struct sockaddr *dest_addr, socklen_t addrlen);
ssize_t k_recvfrom(int sockfd, void *buf, size_t len, int flags, struct sockaddr *src_addr, socklen_t *addrlen);
int k_close(int sockfd);
int dropMessage(float p);

#endif
