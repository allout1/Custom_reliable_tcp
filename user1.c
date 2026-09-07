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
    struct sockaddr_in dest_addr;
    char data_buf[1024];

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
    printf("Enter filename: ");
    scanf("%s", filename);

    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        perror("File open error");
        return 1;
    }

    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port = htons(dest_port);
    inet_aton(dest_ip, &dest_addr.sin_addr);
    printf("Reading file...\n");

    int bytes_read;
    data_buf[0] = '0';  // Data message indicator
    int seq = 1, msg_count = 0;
    while ((bytes_read = read(fd, data_buf + 1, 511)) > 0) {
        printf("Sending %d bytes\n", bytes_read + 1);
        int bytes_sent;
        while (1) {
            while ((bytes_sent = k_sendto(ktp_sock, data_buf, bytes_read + 1, 0,
                                           (struct sockaddr *)&dest_addr, sizeof(dest_addr))) < 0 &&
                   errno == ENOSPACE) {
                sleep(1);
            }
            if (bytes_sent >= 0)
                break;
            perror("Send error");
            return 1;
        }
        printf("Sent %d bytes, seq %d\n", bytes_sent, seq);
        seq = (seq + 1) % 256;
        msg_count++;
    }

    // Send EOF indicator
    data_buf[0] = '1';
    int eof_sent;
    while (1) {
        while ((eof_sent = k_sendto(ktp_sock, data_buf, 1, 0,
                                     (struct sockaddr *)&dest_addr, sizeof(dest_addr))) < 0 &&
               errno == ENOSPACE) {
            sleep(1);
        }
        if (eof_sent >= 0) {
            msg_count++;
            printf("EOF sent. Total messages: %d\n", msg_count);
            break;
        }
        perror("Send error");
        return 1;
    }

    sleep((unsigned int)(300 * P));  // Wait to ensure delivery
    printf("File sent.\n");

    close(fd);
    if (k_close(ktp_sock) < 0) {
        perror("Socket close error");
        return 1;
    }
    return 0;
}
