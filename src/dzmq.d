import zmq;

void fillmessage(void* destination, string data, int size) {
	int i=0;
	while(i<size){
		*cast(char*)(destination++) = data[i++];
	}
}