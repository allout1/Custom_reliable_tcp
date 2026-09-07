#include <stdio.h>
#include "ksocket.h"
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char *argv[]) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <src_IP> <src_port> <dest_IP> <dest_port>\n", argv[0]);
        return 1;
    }

    int ktp_sock;
    struct sockaddr_in sender_addr;
    char recv_buf[512];
    socklen_t addr_len = sizeof(sender_addr);

    // Create KTP socket
    if ((ktp_sock = k_socket(AF_INET, SOCK_KTP, 0)) < 0) {
        perror("Socket creation failed");
        return 1;
    }

    char src_ip[16], dest_ip[16];
    strncpy(src_ip, argv[1], sizeof(src_ip) - 1);
    strncpy(dest_ip, argv[3], sizeof(dest_ip) - 1);
    uint16_t src_port = (uint16_t)atoi(argv[2]);
    uint16_t dest_port = (uint16_t)atoi(argv[4]);

    // Bind the socket with source and destination addresses
    if (k_bind(src_ip, src_port, dest_ip, dest_port) < 0) {
        perror("Bind failed");
        return 1;
    }

    char filename[100];
    snprintf(filename, sizeof(filename), "output_%d.txt", src_port);
    int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        perror("File open error");
        return 1;
    }

    sender_addr.sin_family = AF_INET;
    sender_addr.sin_port = htons(dest_port);
    sender_addr.sin_addr.s_addr = inet_addr(dest_ip);
    printf("Receiving file\n");

    int bytes_recv;
    while (1) {
        while ((bytes_recv = k_recvfrom(ktp_sock, recv_buf, sizeof(recv_buf), 0,
                                         (struct sockaddr *)&sender_addr, &addr_len)) <= 0) {
            printf("Waiting for message...\n");
            sleep(1);
        }
        if (recv_buf[0] == '1') {
            printf("EOF received.\n");
            break;
        }
        printf("Writing %d bytes\n", bytes_recv - 1);
        if (write(fd, recv_buf + 1, bytes_recv - 1) < 0) {
            perror("File write error");
            return 1;
        }
    }

    close(fd);
    sleep((unsigned int)(300 * P));  // Wait to ensure final ACK is processed
    if (k_close(ktp_sock) < 0) {
        perror("Socket close error");
        return 1;
    }
    return 0;
}
