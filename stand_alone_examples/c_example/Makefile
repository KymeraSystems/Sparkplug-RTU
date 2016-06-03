CFLAGS=-Wall -ggdb
LDFLAGS=-lmosquitto -lprotobuf-c

.PHONY: all clean

all : sparkplug_example

sparkplug_example : kurapayload.pb-c.o example.o
	${CC} $^ -o $@ ${LDFLAGS}

kurapayload.pb-c.o : kurapayload.pb-c.c
	${CC} -c $^ -o $@ ${CFLAGS}

example.o : example.c
	${CC} -c $^ -o $@ ${CFLAGS}

clean : 
	-rm -f *.o sparkplug_example
