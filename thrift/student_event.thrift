namespace java com.knewton.thrift

typedef i64 EventId
typedef i64 StudentId
typedef i64 UnixTimeMillis

// The data describing an event.
struct StudentEventData {
	1: required StudentId studentId;
	2: required UnixTimeMillis timestamp;
	3: required string type;
	4: required i32 score;
	5: optional string course;
	6: optional string book;
}

// A Student Event consisting of an id and the data describing the event.
struct StudentEvent {
	1: required EventId id;
	2: required StudentEventData data;
}
