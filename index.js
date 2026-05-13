import express from 'express';
import axios from 'axios';
import db from './db.js';
import { connectToBroker, publishMessage } from './broker.js';

const app = express();
app.use(express.json());

// RabbitMQ
connectToBroker().catch(err => console.error('Broker init error', err));

// Create order
app.post('/', async (req, res) => {
  // TODO: Implement order creation with the following steps:
  // 1. Validate request body:
  //    - Check productId exists
  //    - Check quantity is positive
  // 2. Call product service to verify product exists:
  //    - Use axios to GET product details
  //    - Handle timeouts and errors
  // 3. Insert order into database:
  //    - Add to orders table with PENDING status
  // 4. Publish order.created event to message broker:
  //    - Include order id, product details, quantity
  // 5. Return success response with order details
  try {
    const { productId, quantity } = req.body;

    if (!productId) {
      return res.status(400).json({ error: 'productId is required' });
    }

    if (!quantity || Number(quantity) <= 0) {
      return res.status(400).json({ error: 'quantity must be positive' });
    }

    const productIdNumber = Number(productId);
    const quantityNumber = Number(quantity);

    if (Number.isNaN(productIdNumber) || Number.isNaN(quantityNumber)) {
      return res.status(400).json({
        error: 'productId and quantity must be valid numbers'
      });
    }

    const productServiceUrl =
      process.env.PRODUCT_SERVICE_URL || 'http://product-service:8002';

    let product;

    try {
      const productResponse = await axios.get(
        `${productServiceUrl}/${productIdNumber}`,
        {
          timeout: 5000
        }
      );

      product = productResponse.data;
    } catch (err) {
      if (err.response && err.response.status === 404) {
        return res.status(404).json({ error: 'Product not found' });
      }

      return res.status(503).json({
        error: 'Product service unavailable'
      });
    }

    const result = await db.query(
      `INSERT INTO orders (product_id, quantity, status)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [productIdNumber, quantityNumber, 'PENDING']
    );

    const order = result.rows[0];

    const event = {
      event: 'ORDER_CREATED',
      orderId: order.id,
      productId: order.product_id,
      quantity: order.quantity,
      product
    };

    await publishMessage('order.created', event);

    console.log('Published ORDER_CREATED event to RabbitMQ:', event);

    return res.status(201).json({
      message: 'Order created successfully',
      order
    });
  } catch (err) {
    return res.status(500).json({
      error: 'Internal server error'
    });
  }
});

// List orders
app.get('/', async (_req, res) => {
  const r = await db.query('SELECT * FROM orders ORDER BY id DESC');
  res.json(r.rows);
});

// Get order by id
app.get('/:id', async (req, res) => {
  const id = Number(req.params.id);
  const r = await db.query('SELECT * FROM orders WHERE id = $1', [id]);
  if (r.rows.length === 0) return res.status(404).json({ error: 'Order not found' });
  res.json(r.rows[0]);
});

const PORT = 8003;
app.listen(PORT, () => console.log(`Order Service running on ${PORT}`));
