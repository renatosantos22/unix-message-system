all: broker feed

broker: manager.c shared.h manager_data.h manager_aux.h
	gcc -o manager manager.c manager_aux.c shared.c -pthread

feed: feed.c shared.h feed_data.h
	gcc -o feed feed.c shared.c -pthread

clean:
	rm -f manager feed manager_pipe pipe_feed_*