<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\Conf;

class KafkaConnector implements ConnectorInterface {

    public function connect(array $config)
    {
        $kafkaConf = new Conf();

        $kafkaConf->set('bootstrap.servers', $config['bootstrap_servers']);
        $kafkaConf->set('security.protocol', $config['security_protocol']);
        $kafkaConf->set('sasl.mechanism', $config['sasl_mechanism']);
        $kafkaConf->set('sasl.username', $config['sasl_username']);
        $kafkaConf->set('sasl.password', $config['sasl_password']);

        $kafkaProducer = new Producer($kafkaConf);

        $kafkaConf->set('group.id', $config['group_id']);
        $kafkaConf->set('auto.offset.reset', $config['auto_offset_reset']);

        $kafkaConsumer = new KafkaConsumer($kafkaConf);

        return new KafkaQueue($kafkaProducer, $kafkaConsumer);
    }
}
