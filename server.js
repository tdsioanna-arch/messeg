const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// Подключение к PostgreSQL
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// Список онлайн пользователей
let onlineUsers = [];

// Создание таблиц
async function initDb() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE,
                password TEXT
            )
        `);
        await pool.query(`
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                sender TEXT,
                recipient TEXT,
                message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);
        console.log('База данных PostgreSQL готова');
    } catch (err) {
        console.error('Ошибка инициализации БД:', err);
    }
}

initDb();

// Регистрация
app.post('/register', async (req, res) => {
    const { username, password } = req.body;
    try {
        await pool.query('INSERT INTO users (username, password) VALUES ($1, $2)', [username, password]);
        res.json({ success: true });
    } catch (err) {
        res.status(400).json({ error: 'Пользователь уже существует' });
    }
});

// Вход
app.post('/login', async (req, res) => {
    const { username, password } = req.body;
    try {
        const result = await pool.query('SELECT * FROM users WHERE username = $1 AND password = $2', [username, password]);
        if (result.rows.length > 0) {
            res.json({ success: true, username: result.rows[0].username });
        } else {
            res.status(401).json({ error: 'Неверные данные' });
        }
    } catch (err) {
        res.status(500).json({ error: 'Ошибка сервера' });
    }
});

// Получить список всех пользователей
app.get('/users', async (req, res) => {
    try {
        const result = await pool.query('SELECT username FROM users ORDER BY username');
        res.json(result.rows);
    } catch (err) {
        console.error('Ошибка получения пользователей:', err);
        res.status(500).json({ error: 'Ошибка сервера' });
    }
});

// WebSocket
io.on('connection', (socket) => {
    console.log('Пользователь подключился');

    socket.on('join', (username) => {
        socket.username = username;
        console.log(`${username} присоединился`);
        
        if (!onlineUsers.includes(username)) {
            onlineUsers.push(username);
        }
        io.emit('online users', onlineUsers);
    });

    socket.on('private message', async ({ to, message, from }) => {
        try {
            await pool.query(
                'INSERT INTO messages (sender, recipient, message) VALUES ($1, $2, $3)',
                [from, to, message]
            );
        } catch (err) {
            console.error('Ошибка сохранения:', err);
        }

        const targetSocket = Array.from(io.sockets.sockets.values()).find(
            (s) => s.username === to
        );

        if (targetSocket) {
            targetSocket.emit('private message', {
                from: from,
                message: message,
                timestamp: new Date()
            });
        }

        socket.emit('message sent', { to, message, timestamp: new Date() });
    });

    socket.on('get history', async ({ withUser, currentUser }) => {
        try {
            const result = await pool.query(
                `SELECT * FROM messages 
                 WHERE (sender = $1 AND recipient = $2) OR (sender = $2 AND recipient = $1) 
                 ORDER BY timestamp ASC`,
                [currentUser, withUser]
            );
            socket.emit('message history', { with: withUser, messages: result.rows });
        } catch (err) {
            console.error('Ошибка получения истории:', err);
        }
    });

    socket.on('disconnect', () => {
        console.log('Пользователь отключился');
        if (socket.username) {
            onlineUsers = onlineUsers.filter(u => u !== socket.username);
            io.emit('online users', onlineUsers);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Сервер запущен на порту ${PORT}`);
});