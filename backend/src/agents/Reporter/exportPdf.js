// lib/exportPdf.js
const MarkdownIt = require("markdown-it");
const puppeteer = require("puppeteer");

exports.markdownToPdf = async function (markdown, outPath = "report.pdf") {
  const md = new MarkdownIt({ html: true });
  const rawHtml = md.render(markdown);

  const html = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    body { font-family: Arial, sans-serif; padding: 24px; }
    pre, code { 
      white-space: pre-wrap; 
      word-break: break-word;
      font-size: 13px;
    }
    h1, h2, h3 { margin-top: 28px; }
  </style>
</head>
<body>
${rawHtml}
</body>
</html>
`;

  const browser = await puppeteer.launch({
    headless: "new",
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
    ],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 2000 });

  await page.setContent(html, {
    waitUntil: "networkidle2",
    timeout: 60000,
  });

  await page.pdf({
    path: outPath,
    format: "A4",
    printBackground: true,
  });

  await browser.close();
  return outPath;
};
