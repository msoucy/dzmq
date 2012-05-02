
import core.thread;
import core.time;

import std.stdio;
import std.string;

import zmq;
import dzmq;

void cmain() {
	Context context = new Context(1);
	
	// Socket to talk to server
	writef("Connecting to hello world server…\n");
	Socket requester = new Socket(context, Socket.Type.SUB);
	requester.connect("tcp://localhost:5555");
	requester.subscribe("ZMQTesting");
	
	int request_nbr;
	string topic;
	for (request_nbr = 0; request_nbr != 10; request_nbr++) {
		string[] s = requester.recv_topic(topic);
		writef("Received %s: %s (%d)\n", topic, s, request_nbr);
	}
}

void smain()
{
	Context context = new Context(1);
	
	// Socket to talk to clients
	Socket responder = new Socket(context, Socket.Type.PUB);
	responder.bind("tcp://*:5555");
	
	int i=0;
	while (1) {
		// Wait for next request from client
		responder.send_topic("ZMQTesting", format("%d",i++));
		
		// Do some 'work'
		Thread.sleep(dur!"seconds"(1));
	}
}

void main(string[] argv) {
	if(argv.length != 2) {
		stderr.writeln("Error: Invalid arguments");
		return;
	}
	if(argv[1] == "server") smain();
	else cmain();
}
