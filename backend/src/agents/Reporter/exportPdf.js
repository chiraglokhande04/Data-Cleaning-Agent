// lib/exportPdf.js
const MarkdownIt = require("markdown-it");
const puppeteer = require("puppeteer");

exports.markdownToPdf = async function(markdown, outPath = "report.pdf") {
  const md = new MarkdownIt({ html: true });
  const html = md.render(markdown);

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  const page = await browser.newPage();
  await page.setContent(html, { waitUntil: "networkidle0" });
  await page.pdf({ path: outPath, format: "A4", printBackground: true });
  await browser.close();
  return outPath;
}
