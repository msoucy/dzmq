
import std.stdio;

import zmq;
import dzmq;

void cmain() {
	Context context = new Context(1);
	
	// Socket to talk to server
	writef("Connecting to hello world server…\n");
	Socket requester = new Socket(context, Socket.Type.REQ);
	requester.connect("tcp://localhost:5555");
	
	int request_nbr;
	for (request_nbr = 0; request_nbr != 10; request_nbr++) {
		writef ("Sending Hello %d…\n", request_nbr);
		requester.send("Hello");
		
		string s = requester.recv();
		writef("Received %s %d\n", s, request_nbr);
	}
}

void smain()
{
	Context context = new Context(1);
	
	// Socket to talk to clients
	Socket responder = new Socket(context, Socket.Type.REP);
	responder.bind("tcp://*:5555");
	
	while (1) {
		// Wait for next request from client
		string s = responder.recv();
		writef("Received %s\n",s);
		
		// Do some 'work'
		//sleep (1);
		foreach(i;0..100000){}
		
		// Send reply back to client
		responder.send("World");
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
