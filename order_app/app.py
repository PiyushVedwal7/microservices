from flask import Flask, render_template, request, redirect, url_for, flash
from kafka import KafkaProducer
import json
import secrets

app = Flask(__name__)

# Securely generate a secret key
app.secret_key = secrets.token_hex(16)  # Generates a 32-character secure hex key

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/place_order', methods=['POST'])
def place_order():
    order_id = request.form['order_id']
    product = request.form['product']
    quantity = request.form['quantity']

    # Create the order object
    order_data = {
        'order_id': order_id,
        'product': product,
        'quantity': int(quantity)
    }

    # Send to Kafka
    producer.send('order_topic', order_data)
    producer.flush()

    # Send success message back to the UI
    flash('Order placed successfully!')
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
