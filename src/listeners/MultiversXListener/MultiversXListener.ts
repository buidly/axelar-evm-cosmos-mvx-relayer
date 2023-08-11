import { env } from '../../config';
import { logger } from '../../logger';
import { BlockEvent, NotifierEvent } from './types';
import {
  AbiRegistry, Address,
  AddressType, ArrayVecType,
  BinaryCodec, BytesType,
  FieldDefinition,
  StringType,
  StructType
} from '@multiversx/sdk-core/out';
import { promises } from "fs";

const amqp = require('amqplib/channel_api.js');

export class MultiversXListener {
  async listenToMultiversXEvents() {
    const connection = await amqp.connect(env.MULTIVERSX_NOTIFIER_URL, {});

    const channel = await connection.createChannel();

    const queue = await channel.checkQueue('events-638c4d10');

    await channel.consume(queue.queue, async (msg: any) => {
      const blockEvent: BlockEvent = JSON.parse(msg.content.toString());

      for (const event of blockEvent.events) {
        await this.handleEvent(event);
      }
    });

    logger.info('[MultiversXListener] Listening to MultiversX Gateway events');
  }

  private async handleEvent(event: NotifierEvent) {
    if (event.address !== env.MULTIVERSX_GATEWAY_ADDRESS) {
      return;
    }

    if (event.identifier === 'callContract') {
      logger.info('[MultiversXListener] Received callContract event from MultiversX Gateway contract:');
      logger.info(JSON.stringify(event));

      const sender = Address.fromBuffer(Buffer.from(event.topics[1], 'base64'));
      const destinationChain = Buffer.from(event.topics[2], 'base64').toString();
      const destinationContractAddress = Buffer.from(event.topics[3], 'base64').toString('hex');

      const attributes = Buffer.from(event.data, 'base64');
      const dataHash = attributes.slice(0, 32).toString('hex');
      const dataPayloadBuffer = attributes.slice(32);

      const codec = new BinaryCodec();
      const [decoded] = codec.decodeNested(dataPayloadBuffer, new BytesType());
      const dataPayload = (decoded.valueOf() as Buffer).toString('hex');

      logger.info(`Event contains data: ${sender} ${destinationChain} ${destinationContractAddress} ${dataHash} ${dataPayload}`);
    }
  }
}
