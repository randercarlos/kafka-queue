<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

class KafkaQueue extends Queue implements QueueContract
{
    protected KafkaConsumer $kafkaConsumer;
    protected Producer $kafkaProducer;

    public function __construct(Producer $kafkaProducer, KafkaConsumer $kafkaConsumer) {
        $this->kafkaConsumer = $kafkaConsumer;
        $this->kafkaProducer = $kafkaProducer;
    }

    public function size($queue = null)
    {
    }

    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->kafkaProducer->newTopic($queue ?? env('KAFKA_QUEUE', 'default'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->kafkaProducer->flush(1000);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
    }

    public function pop($queue = null)
    {
    	try {
			$this->kafkaConsumer->subscribe([$queue]);
			$message = $this->kafkaConsumer->consume(-1);

			$this->handleKafkaResponse($message);
		} catch(\Exception $exception) {
    		var_dump($exception->getMessage());
		}
    }

    private function handleKafkaResponse($message): void {
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
				$job = unserialize($message->payload);
				$job->handle();
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                var_dump('No more messages... Waiting for more...');
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
				var_dump('Timed out');
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
        }
    }
}
