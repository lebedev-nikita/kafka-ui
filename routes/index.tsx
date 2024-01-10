import { FreshContext } from "$fresh/server.ts";
import { Kafka } from "npm:kafkajs@2.2.4";

async function fetchData(ctx: FreshContext) {
  const brokers = ctx.url.searchParams.get("brokers");
  const topic = ctx.url.searchParams.get("topic");

  const messages: { key: string | null; value: string | null }[] = [];
  const errors: string[] = [];
  const availableTopics: string[] = [];

  if (brokers) {
    const kafkaBrokers = brokers.split(",");
    console.log({ kafkaBrokers });

    const kafka = new Kafka({
      clientId: crypto.randomUUID(),
      brokers: kafkaBrokers,
      retry: { retries: 0 },
      ssl: false,
    });

    const admin = kafka.admin();
    try {
      await admin.connect();
      for (const topic of await admin.listTopics()) {
        availableTopics.push(topic);
      }
    } catch (_error) {
      if (_error instanceof Error) {
        errors.push(_error.message);
      }
    } finally {
      await admin.disconnect();
    }

    if (topic) {
      const consumer = kafka.consumer({ groupId: "test-group" });

      try {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: true });
        await consumer.run({
          async eachMessage({ message }) {
            const key = message.key && message.key.toString();
            const value = message.value && message.value.toString();
            messages.push({ key, value });
          },
        });
      } catch (_error) {
        if (_error instanceof Error) {
          errors.push(_error.message);
        }
      } finally {
        await consumer.disconnect();
      }
    }
  }

  return { brokers, topic, errors, messages, availableTopics };
}

export default async function Home(req: Request, ctx: FreshContext) {
  const data = await fetchData(ctx);

  return (
    <div className="px-4 py-8 mx-auto bg-[#86efac] min-h-screen flex flex-col gap-2">
      <form className="bg-red-400 p-2 flex flex-col gap-1 rounded-md">
        <label className="flex gap-2">
          brokers:
          <input
            name="brokers"
            defaultValue={data.brokers ?? ""}
            className="grow"
          />
        </label>

        <fieldset className="flex flex-col">
          {data.availableTopics.map((name) => (
            <label>
              <input type="radio" value={name} name="topic" />
              <span>
                {" " + name}
              </span>
            </label>
          ))}
        </fieldset>
        <button
          type="submit"
          className="rounded-sm self-start px-2 py-0.5 bg-slate-200 hover:bg-slate-300 active:scale-95"
        >
          connect
        </button>
      </form>

      {data.errors.map((e, index) => (
        <p key={index} className="bg-red-400 rounded-md py-1 px-2">
          <b>{"Error: "}</b>
          {e}
        </p>
      ))}

      <pre>{JSON.stringify(data, null, 2)}</pre>
    </div>
  );
}
