#include "segel.h"
#include "request.h"
#include "log.h"

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//

// Parses command-line arguments
void getargs(int *port, int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(1);
    }
    *port = atoi(argv[1]);
}

// TODO: HW3 — Task 1: Initialize the thread pool and request queue.
// This server currently handles all requests in the main thread.
typedef struct workerThread{
    pthread_t thread;
    struct Threads_stats thread_stats;
} activeThread;

typedef struct {
    int connfd;
    time_stats task_time_stats;
} incomingTask;

activeThread *threads_array = NULL;

incomingTask* tasks_queue = NULL;
int max_queue_size;
int head = 0;
int tail = 0;
int incoming_tasks_amount = 0;
int active_tasks_amount = 0;
pthread_mutex_t lock;
pthread_cond_t queue_not_full;
pthread_cond_t queue_not_empty;

server_log log_instance;

// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).

// TODO: HW3 — Extend getargs() to parse the full argument list.
void extended_getargs(int *tcp_port,int *udp_port,int *threads_size,
                            int *queue_size,double *debug_time ,int argc, char *argv[])
{
    if (argc < 6) {
        fprintf(stderr, "Usage: %s <tcp_port> <udp_port> <threads> <queue_size> <debug_sleep_time>\n", argv[0]);
        exit(1);
    }
    *tcp_port = atoi(argv[1]);
    *udp_port = atoi(argv[2]);
    *threads_size = atoi(argv[3]);
    *queue_size = atoi(argv[4]);
    *debug_time = atof(argv[5]);
}


void* thread_function(void* at){
    activeThread* this_thread = at; //info about what this thread is for stat update.

    while(1){
        //lock and go into queue to search for the next task.
        pthread_mutex_lock(&lock);
        while (incoming_tasks_amount == 0) {
            pthread_cond_wait(&queue_not_empty, &lock); //if empty, wait and release the lock.
        }

        //if at least one task is abailable, take the first, update head to the next, add 1 to active and decrease 1 from incoming tasks.
        incomingTask task = tasks_queue[head];
        head = (head + 1) % max_queue_size;
        incoming_tasks_amount--;
        active_tasks_amount++;

        pthread_mutex_unlock(&lock); //release lock, then update time of dispatch and start handeling.

        gettimeofday(&task.task_time_stats.task_dispatch, NULL);
        requestHandle(task.connfd, task.task_time_stats, &this_thread->thread_stats, log_instance);

        //job is done, close the socketfd.
        Close(task.connfd);

        //deacrease 1 from the active tasks counter, and signal that the queue is not full, if main thread is waiting on it.
        pthread_mutex_lock(&lock);
        active_tasks_amount--;
        pthread_cond_signal(&queue_not_full);
        pthread_mutex_unlock(&lock);
    }
}




int main(int argc, char *argv[])
{
    // Create the global server log
    log_instance = create_log();

    // int port;
    int listenfd, connfd, clientlen;
    int tcp_port, udp_port, threads_size, queue_size;
    struct sockaddr_in clientaddr;
    double debug_time;

    // getargs(&port, argc, argv);
    extended_getargs(&tcp_port, &udp_port, &threads_size, &queue_size, &debug_time, argc, argv);
    
    max_queue_size = queue_size;

    //initialize lock and conditions
    pthread_mutex_init(&lock, NULL);
    pthread_cond_init(&queue_not_full, NULL);
    pthread_cond_init(&queue_not_empty, NULL);

    //set threads pool and queue;
    threads_array = malloc(sizeof(activeThread)*threads_size);
    tasks_queue =  malloc(sizeof(incomingTask)*queue_size);

    //create threads.
    for (int i = 0; i < threads_size; i++){
        threads_array[i].thread_stats.id = i;
        threads_array[i].thread_stats.dynm_req = 0;
        threads_array[i].thread_stats.post_req = 0;
        threads_array[i].thread_stats.stat_req = 0;
        threads_array[i].thread_stats.total_req = 0;
        if (pthread_create(&threads_array[i].thread, NULL, thread_function , &threads_array[i]) != 0){
            //creating the thread went wrong.
            free(threads_array);
            free(tasks_queue);
            exit(1);
        }
    }


    listenfd = Open_listenfd(tcp_port);
    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);


        incomingTask new_task;
        new_task.connfd = connfd;

        // TODO: HW3 — Record the request arrival time here.
        gettimeofday(&new_task.task_time_stats.task_arrival, NULL);

        // // DEMO PURPOSE ONLY:
        // // This is a dummy request handler that immediately processes the
        // // request in the master thread without concurrency. Replace this with
        // // logic that enqueues the connection so a worker thread handles it.

        // threads_stats t = malloc(sizeof(struct Threads_stats));
        // t->id = 0;             // Thread ID (placeholder)
        // t->stat_req = 0;       // Static request count
        // t->dynm_req = 0;       // Dynamic request count
        // t->post_req = 0;       // POST request count
        // t->total_req = 0;      // Total request count


        //lock, add the incoming task only if there is space in queue, otherwise wait.
        //then signal the threads waiting on not empty that there is a taks to be done and then unlock.
        //we delegated the requesthandle the the threads, the will also update if queue is empty or full if needed.
        //and they are also responsible to close the fd given to them through new_task.connfd at the end.
        //they also need to update dispatch and thread_stats via 
        pthread_mutex_lock(&lock);
        while ((incoming_tasks_amount + active_tasks_amount) >= max_queue_size) {
            pthread_cond_wait(&queue_not_full, &lock);
        }

        tasks_queue[tail] = new_task;
        tail = (tail + 1) % max_queue_size;
        incoming_tasks_amount++;
        pthread_cond_signal(&queue_not_empty);
        pthread_mutex_unlock(&lock);

        // time_stats dum;

        // gettimeofday(&arrival, NULL);

        // Call the request handler (immediate in master thread — DEMO ONLY)
        // requestHandle(connfd, dum, t, log);

        // free(t); // Cleanup
        // Close(connfd); // Close the connection
    }

    // Clean up the server log before exiting
    destroy_log(log_instance);

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
    free(tasks_queue);
    free(threads_array);
}
