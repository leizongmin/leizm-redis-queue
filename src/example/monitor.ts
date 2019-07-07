/**
 * super-queue example
 *
 * @authro Zongmin Lei <leizongmin@gmail.com>
 */

import { Monitor } from "../lib";

const m = new Monitor({
  redis: {
    host: "127.0.0.1",
    port: 6379,
    prefix: "example:",
  },
});

// setInterval(() => m.check(), 2000);
// setInterval(() => m.clientStatus(console.log), 2000);
// setInterval(() => m.queueStatus(console.log), 2000);

m.on("producerUp", n => console.log("producerUp", n));
m.on("producerDown", n => console.log("producerDown", n));
m.on("consumerUp", n => console.log("consumerUp", n));
m.on("consumerDown", n => console.log("consumerDown", n));
