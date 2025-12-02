const express = require("express");
const cors = require("cors");
require('dotenv').config();

const filesRouter = require("./routes/files.js");
const cleanRouter = require("./routes/clean.js")
const connectDB = require("./db/connectDB.js");


const app = express();

const PORT = process.env.PORT || 3000;

app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "PUT", "DELETE"],
  allowedHeaders: ["Content-Type", "Authorization"]
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));


connectDB()

app.get("/", (req, res) => {
  res.json({ message: "Express server is working 🚀" });
});

app.use('/api/files',filesRouter)
app.use('/api/datasets',cleanRouter)

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});