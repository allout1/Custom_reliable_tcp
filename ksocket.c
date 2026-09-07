#include "ksocket.h"

SHM* SM;                   // Shared memory for KTP sockets
SOCKET_info* socket_info;  // Shared info structure
struct sembuf pop, vop;    // Semaphore operations
int sem1, sem2;            // Semaphores for initksocket interaction
int sem_SM;                // Semaphore for shared memory
int sem_socket_info;       // Semaphore for socket info
int shmid_SM;              // Shared memory ID for SM
int shmid_socket_info;     // Shared memory ID for socket_info

void get_shared_mem() {
    int key_sm = ftok("ksocket.h", 'a');
    int key_sockinfo = ftok("ksocket.h", 'b');
    int key_sem1 = ftok("ksocket.h", 'c');
    int key_sem2 = ftok("ksocket.h", 'd');
    int key_sem_sm = ftok("ksocket.h", 'e');
    int key_sem_sockinfo = ftok("ksocket.h", 'f');

    shmid_SM = shmget(key_sm, sizeof(SHM) * N, 0777);
    if (shmid_SM == -1) {
        perror("Error obtaining SM shared memory");
        exit(1);
    }
    SM = (SHM*)shmat(shmid_SM, NULL, 0);
    if (SM == (void*)-1) {
        perror("Error attaching SM shared memory");
        exit(1);
    }

    shmid_socket_info = shmget(key_sockinfo, sizeof(SOCKET_info), 0777);
    if (shmid_socket_info == -1) {
        perror("Error obtaining socket_info shared memory");
        exit(1);
    }
    socket_info = (SOCKET_info*)shmat(shmid_socket_info, NULL, 0);
    if (socket_info == (void*)-1) {
        perror("Error attaching socket_info shared memory");
        exit(1);
    }

    sem1 = semget(key_sem1, 1, 0777);
    sem2 = semget(key_sem2, 1, 0777);
    sem_SM = semget(key_sem_sm, 1, 0777);
    sem_socket_info = semget(key_sem_sockinfo, 1, 0777);
    if (sem1 == -1 || sem2 == -1 || sem_SM == -1 || sem_socket_info == -1) {
        perror("Error obtaining semaphores");
        exit(1);
    }

    pop.sem_num = pop.sem_flg = vop.sem_num = vop.sem_flg = 0;
    pop.sem_op = -1;
    vop.sem_op = 1;
}

int find_free_entry() {
    waitSem(sem_SM);
    for (int i = 0; i < N; i++) {
        if (SM[i].free == 1) {
            signalSem(sem_SM);
            return i;
        }
    }
    signalSem(sem_SM);
    return -1;
}

int k_socket(int domain, int type, int protocol) {
    if (type != SOCK_KTP) {
        errno = EINVAL;
        return -1;
    }
    get_shared_mem();
    int idx;
    waitSem(sem_socket_info);
    idx = find_free_entry();
    if (idx == -1) {
        errno = ENOSPACE;
        socket_info->sock_id = 0;
        socket_info->err_no = ENOSPACE;
        strcpy(socket_info->ip_addr, "\0");
        socket_info->port = 0;
        signalSem(sem_socket_info);
        return -1;
    }
    signalSem(sem_socket_info);

    signalSem(sem1);
    waitSem(sem2);

    waitSem(sem_socket_info);
    if (socket_info->sock_id == -1) {
        errno = socket_info->err_no;
        socket_info->sock_id = 0;
        socket_info->err_no = 0;
        strcpy(socket_info->ip_addr, "\0");
        socket_info->port = 0;
        signalSem(sem_socket_info);
        return -1;
    }
    signalSem(sem_socket_info);

    waitSem(sem_SM);
    SM[idx].free = 0;
    SM[idx].pid = getpid();
    SM[idx].udp_socket_id = socket_info->sock_id;
    for (int j = 0; j < MAX_SEQ_NUM; j++) {
        SM[idx].swnd.arr[j] = -1;
        SM[idx].rwnd.arr[j] = (j <= MAX_WINDOW_SIZE && j > 0) ? j - 1 : -1;
        SM[idx].last_sent_time[j] = -1;
    }
    SM[idx].swnd.base = 1;
    SM[idx].rwnd.base = 1;
    SM[idx].swnd.size = MAX_WINDOW_SIZE;
    SM[idx].rwnd.size = MAX_WINDOW_SIZE;
    SM[idx].send_buffer_size = MAX_WINDOW_SIZE;
    for (int j = 0; j < MAX_WINDOW_SIZE; j++)
        SM[idx].recv_buffer_valid[j] = 0;
    SM[idx].recv_seq_num = 0;
    SM[idx].nospace = 0;
    signalSem(sem_SM);

    waitSem(sem_socket_info);
    socket_info->sock_id = 0;
    socket_info->err_no = 0;
    strcpy(socket_info->ip_addr, "\0");
    socket_info->port = 0;
    signalSem(sem_socket_info);

    return idx;
}

int k_bind(char src_ip[], uint16_t src_port, char dest_ip[], uint16_t dest_port) {
    get_shared_mem();
    int sockfd = -1;
    waitSem(sem_SM);
    for (int i = 0; i < N; i++) {
        if (!SM[i].free && SM[i].pid == getpid()) {
            sockfd = i;
            break;
        }
    }
    waitSem(sem_socket_info);
    if (sockfd == -1 || sockfd == N) {
        errno = ENOSPACE;
        socket_info->sock_id = 0;
        socket_info->err_no = ENOTBOUND;
        strcpy(socket_info->ip_addr, "\0");
        socket_info->port = 0;
        signalSem(sem_socket_info);
        signalSem(sem_SM);
        return -1;
    }
    socket_info->sock_id = SM[sockfd].udp_socket_id;
    strcpy(socket_info->ip_addr, src_ip);
    socket_info->port = src_port;
    signalSem(sem_socket_info);

    signalSem(sem1);
    waitSem(sem2);

    waitSem(sem_socket_info);
    if (socket_info->sock_id == -1) {
        errno = socket_info->err_no;
        socket_info->sock_id = 0;
        socket_info->err_no = 0;
        strcpy(socket_info->ip_addr, "\0");
        socket_info->port = 0;
        signalSem(sem_socket_info);
        signalSem(sem_SM);
        return -1;
    }
    signalSem(sem_socket_info);

    strcpy(SM[sockfd].remote_ip, dest_ip);
    SM[sockfd].remote_port = dest_port;
    signalSem(sem_SM);

    waitSem(sem_socket_info);
    socket_info->sock_id = 0;
    socket_info->err_no = 0;
    strcpy(socket_info->ip_addr, "\0");
    socket_info->port = 0;
    signalSem(sem_socket_info);

    return 0;
}

ssize_t k_sendto(int sockfd, const void *buf, size_t len, int flags, const struct sockaddr *dest_addr, socklen_t addrlen) {
    get_shared_mem();
    waitSem(sem_SM);
    
    char *dest_ip = inet_ntoa(((struct sockaddr_in*)dest_addr)->sin_addr);
    uint16_t dest_port = ntohs(((struct sockaddr_in*)dest_addr)->sin_port);
    if (strcmp(SM[sockfd].remote_ip, dest_ip) != 0 || SM[sockfd].remote_port != dest_port) {
        printf("Destination mismatch.\n");
        errno = ENOTBOUND;
        signalSem(sem_SM);
        return -1;
    }
    if (SM[sockfd].send_buffer_size == 0) {
        errno = ENOSPACE;
        signalSem(sem_SM);
        return -1;
    }
    int seq = SM[sockfd].swnd.base;
    while (SM[sockfd].swnd.arr[seq] != -1)
        seq = (seq + 1) % MAX_SEQ_NUM;
    int buffer_index = 0, found = 0;
    for (buffer_index = 0; buffer_index < MAX_WINDOW_SIZE; buffer_index++) {
        found = 1;
        for (int i = 0; i < MAX_SEQ_NUM; i++) {
            if (SM[sockfd].swnd.arr[i] == buffer_index) {
                found = 0;
                break;
            }
        }
        if (found) break;
    }
    if (!found) {
        errno = ENOSPACE;
        printf("No available buffer index found.\n");
        signalSem(sem_SM);
        return -1;
    }
    SM[sockfd].swnd.arr[seq] = buffer_index;
    memcpy(SM[sockfd].send_buffer[buffer_index], buf, len);
    SM[sockfd].len_send_buffer[buffer_index] = len;
    SM[sockfd].send_buffer_size--;
    SM[sockfd].last_sent_time[seq] = -1;
    signalSem(sem_SM);
    return len;
}

ssize_t k_recvfrom(int sockfd, void *buf, size_t len, int flags, struct sockaddr *src_addr, socklen_t *addrlen) {
    get_shared_mem();
    waitSem(sem_SM);
    if (sockfd < 0 || sockfd >= N || SM[sockfd].free) {
        errno = EBADF;
        signalSem(sem_SM);
        return -1;
    }
    SHM *sm = &SM[sockfd];
    if (sm->recv_buffer_valid[sm->recv_seq_num] == 0) {
        errno = ENOMESSAGE;
        signalSem(sem_SM);
        return -1;
    }
    sm->recv_buffer_valid[sm->recv_seq_num] = 0;
    sm->rwnd.size++;
    int seq = -1;
    for (int i = 0; i < MAX_SEQ_NUM; i++) {
        if (sm->rwnd.arr[i] == sm->recv_seq_num) {
            seq = i;
            break;
        }
    }
    sm->rwnd.arr[seq] = -1;
    sm->rwnd.arr[(seq + MAX_WINDOW_SIZE) % MAX_SEQ_NUM] = sm->recv_seq_num;
    int msg_len = sm->len_recv_buffer[sm->recv_seq_num];
    printf("Received message length = %d\n", msg_len);
    memcpy(buf, sm->recv_buffer[sm->recv_seq_num], (len < msg_len) ? len : msg_len);
    sm->recv_seq_num = (sm->recv_seq_num + 1) % MAX_WINDOW_SIZE;
    signalSem(sem_SM);
    return (len < msg_len) ? len : msg_len;
}

int k_close(int sockfd) {
    get_shared_mem();
    waitSem(sem_SM);
    SM[sockfd].free = 1;
    signalSem(sem_SM);
    return 0;
}

int dropMessage(float p) {
    float r = (float)rand() / (float)RAND_MAX;
    return (r < p) ? 1 : 0;
}
