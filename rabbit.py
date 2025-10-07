import pika

HOST, PORT = "10.0.0.158", 5672
USER, PASS, VHOST = "livrario", "Livrario111!!!", "/"

params = pika.ConnectionParameters(
    host=HOST, port=PORT, virtual_host=VHOST,
    credentials=pika.PlainCredentials(USER, PASS)
)
conn = pika.BlockingConnection(params)
ch = conn.channel()

# 1) Exchange topic
ch.exchange_declare("gen.topic", exchange_type="topic", durable=True)

# 2) Colas (durables)
queues = {
    "gen_descripciones": "gen.descripciones.*",
    "gen_personajes":    "gen.personajes.*",
    "gen_lugares":       "gen.lugares.*"
}
for q, rk in queues.items():
    ch.queue_declare(q, durable=True, exclusive=False, auto_delete=False)
    ch.queue_bind(q, "gen.topic", rk)

print("Topología creada/asegurada ✅")
conn.close()