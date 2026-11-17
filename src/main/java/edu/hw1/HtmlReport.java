package edu.hw1;

import edu.hw1.Protocol.Row;
import java.net.URI;

final class HtmlReport {
    static String render(Row[] rows) {
        StringBuilder html = new StringBuilder("""
                <!doctype html>
                <html lang="en"><head><meta charset="utf-8"><title>OCR</title>
                <style>.ocr-text { white-space: pre-wrap; }</style></head>
                <body>
                """);
        for (Row row : rows) {
            String url = escape(row.url());
            html.append("<p>");
            if (webUrl(row.url())) {
                html.append("<a href=\"").append(url).append("\">");
                if (row.error() == null) {
                    html.append("<img src=\"").append(url).append("\" alt=\"Original image\">");
                } else {
                    html.append(url);
                }
                html.append("</a>");
            } else {
                html.append(url);
            }
            if (row.error() == null) {
                html.append("<br><span class=\"ocr-text\">").append(escape(row.text())).append("</span>");
            } else {
                html.append("<br>Error: ").append(escape(row.error()));
            }
            html.append("</p>\n");
        }
        return html.append("</body></html>\n").toString();
    }

    private static boolean webUrl(String url) {
        try {
            URI uri = URI.create(url);
            return uri.getHost() != null && ("http".equalsIgnoreCase(uri.getScheme())
                    || "https".equalsIgnoreCase(uri.getScheme()));
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static String escape(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&#39;");
    }
}
