CFLAGS=-Wall -ggdb
#LDFLAGS=../../lib/libmosquitto.so.1 -lmysqlclient
LDFLAGS=-lmosquitto -lprotobuf-c

.PHONY: all clean

all : chariot_example

chariot_example : kurapayload.pb-c.o example.o
	${CC} $^ -o $@ ${LDFLAGS}

kurapayload.pb-c.o : kurapayload.pb-c.c
	${CC} -c $^ -o $@ ${CFLAGS}

example.o : example.c
	${CC} -c $^ -o $@ ${CFLAGS}

clean : 
	-rm -f *.o chariot_example
