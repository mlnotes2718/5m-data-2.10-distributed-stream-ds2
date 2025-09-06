import streamlit as st
from confluent_kafka import Consumer, KafkaError

# Kafka Consumer configuration
conf = {
    'bootstrap.servers': 'pkcxxx9092',   # from Confluent Cloud
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'xxx',     # from Confluent Cloud
    'sasl.password': 'xxx',  # from Confluent Cloud
    'group.id': 'streamlit-consumer-debug-004',   # new group to avoid reusing old offsets
    'auto.offset.reset': 'earliest'              # start from earliest if no committed offset
}

# Initialize consumer once and keep it across reruns
if "consumer" not in st.session_state:
    st.session_state.consumer = Consumer(conf)
    st.session_state.consumer.subscribe(['sctp_3'])

consumer = st.session_state.consumer

st.title("📡 Kafka Consumer Dashboard")

if st.button("Poll Kafka Messages"):
    messages = []
    # Try to poll multiple times in one click
    for _ in range(10):   # poll more times than before
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                st.error(f"Kafka error: {msg.error()}")
        else:
            decoded = msg.value().decode("utf-8")
            messages.append(decoded)

    if messages:
        st.write("### New Messages:")
        for m in messages:
            st.success(m)
    else:
        st.warning("No new messages found.")

# Do NOT close the consumer here – keep it alive
