const mongoose = require("mongoose")

const connectDB = async () => {
  try {
    // const conn = await mongoose.connect(process.env.MONGO_URI, {
    //   dbName: "PixxelAI",
    //   useNewUrlParser: true,
    //   useUnifiedTopology: true,
    // });

     const conn = await mongoose.connect(process.env.MONGO_URI)

    // const conn = await mongoose.connect(process.env.MONGO_URI, {
   
    //   useNewUrlParser: true,
    //   useUnifiedTopology: true,
    // });

    console.log(`✅ MongoDB connected: ${conn.connection.host}`);
  } catch (error) {
    console.error("❌ MongoDB connection failed:", error.message);
    process.exit(1); // Exit with failure
  }
};

module.exports = connectDB