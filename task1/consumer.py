from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    c = Consumer(config)

    c.subscribe(['mytopic'])

    numlist = []
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))

        numlist.append(int(msg.value()))
        if len(numlist) == 5:
            print('sum: {}'.format(sum(numlist)))
            numlist.clear()
    c.close()
