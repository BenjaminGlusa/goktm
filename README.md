# Go Kafka Topic Mover

The Go Kafka Topic Mover aims to move all messages of an Apache Kafka Topic to another one as backup or cluster migration.

At the moment the target is an aws msk cluster. So goktm assumes that there is an iam role to use for authentication.

Furthermore, the messages are stored in a s3 bucket before they are re-published to the new topic/cluster. 

**IMPORTANT**

The project is in an early proof of concept state and **should not be used in production**. 
