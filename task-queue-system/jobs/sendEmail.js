const nodemailer = require("nodemailer");
require("dotenv").config();

const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS
    }
});

transporter.verify((error) => {
    if (error) {
        console.log("SMTP error:", error);
    } else {
        console.log("SMTP server ready");
    }
});

async function sendEmail(payload) {

    console.log("Sending email to:", payload.to);

    const info = await transporter.sendMail({
        from: `<${process.env.EMAIL_USER}>`,
        to: payload.to,
        subject: payload.subject,
        html: payload.html
    });

    console.log("Email sent:", info.messageId);
}

module.exports = sendEmail;
