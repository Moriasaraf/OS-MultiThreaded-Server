#include <stdlib.h>
#include <string.h>
#include "log.h"

// Opaque struct definition
struct Server_Log {
    // TODO: Implement internal log storage (e.g., dynamic buffer, linked list, etc.)
    char* buffer;
    int buff_length;
    int buff_size;

    int active_readers;
    int active_writers;
    int waiting_writers;
    pthread_mutex_t log_lock;
    pthread_cond_t read_cond;
    pthread_cond_t write_cond;
};

// Creates a new server log instance (stub)
server_log create_log() {
    // TODO: Allocate and initialize internal log structure

    //using dynamic buffer, put \0 at beginning so we can use strlen.
    struct Server_Log* temp = malloc(sizeof(struct Server_Log));
    int initial_buff_size = 128;

    temp->buffer = (char*)malloc(initial_buff_size);
    temp->buffer[0] = '\0';
    temp->buff_length = 0;
    temp->buff_size = initial_buff_size;

    temp->active_readers = 0;
    temp->active_writers = 0;
    temp->waiting_writers = 0;
    pthread_mutex_init(&temp->log_lock, NULL);
    pthread_cond_init(&temp->read_cond, NULL);
    pthread_cond_init(&temp->write_cond, NULL);
    
    return temp;
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    // TODO: Free all internal resources used by the log
    if (log == NULL){
        return;
    }
    free(log->buffer);
    pthread_mutex_destroy(&log->log_lock);
    pthread_cond_destroy(&log->read_cond);
    pthread_cond_destroy(&log->write_cond);
    free(log);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst) {
    // TODO: Return the full contents of the log as a dynamically allocated string
    // This function should handle concurrent access

    // const char* dummy = "Log is not implemented.\n";
    // int len = strlen(dummy);
    pthread_mutex_lock(&log->log_lock);
    while (log->active_writers > 0 || log->waiting_writers > 0){
        pthread_cond_wait(&log->read_cond, &log->log_lock);  
    }
    log->active_readers++;
    pthread_mutex_unlock(&log->log_lock);

    *dst = (char*)malloc(log->buff_length + 1); // Allocate for caller
    if (*dst != NULL) {
        strcpy(*dst, log->buffer);
    }

    pthread_mutex_lock(&log->log_lock);
    log->active_readers--;
    if (log->active_readers == 0) pthread_cond_broadcast(&log->write_cond);
    pthread_mutex_unlock(&log->log_lock);

    return log->buff_length;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    // TODO: Append the provided data to the log
    // This function should handle concurrent access
    pthread_mutex_lock(&log->log_lock);
    log->waiting_writers++;

    while (log->active_writers > 0 || log->active_readers > 0){
        pthread_cond_wait(&log->write_cond, &log->log_lock);
    }
    log->active_writers++;
    log->waiting_writers--;

    //inset to buff, the release lock and broadcast read_cond and write_cond
    if (log->buff_size < log->buff_length + data_len + 2){
        log->buffer = realloc(log->buffer, 2*(log->buff_length + data_len + 2));
        log->buff_size = 2*(log->buff_length + data_len + 2);
    }
    strcat(log->buffer, data);
    strcat(log->buffer, "#");
    
    log->buff_length = log->buff_length + strlen(data) + 1;

    log->active_writers--;

    //decide if wake up writers or readers.
    if (log->waiting_writers > 0){
        pthread_cond_signal(&log->write_cond);
    }else{
        pthread_cond_broadcast(&log->read_cond);
    }
    pthread_mutex_unlock(&log->log_lock);
}
