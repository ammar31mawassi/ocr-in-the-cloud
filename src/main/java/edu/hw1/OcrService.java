package edu.hw1;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import javax.imageio.ImageIO;
import net.sourceforge.tess4j.Tesseract;

final class OcrService {
    private final LocalOcr localOcr;
    private final Path tessdata;

    OcrService(Path tessdata) {
        this.tessdata = tessdata.toAbsolutePath();
        localOcr = System.getenv("HW1_TESSERACT_BIN") == null ? new LocalOcr(this.tessdata) : null;
    }

    String extractText(BufferedImage image) throws Exception {
        if (localOcr != null) return localOcr.extractText(image);
        Path input = Files.createTempFile("ocr-input-", ".png");
        try {
            Path output = input.resolveSibling(input.getFileName() + ".txt");
            try {
                ImageIO.write(image, "png", input.toFile());
                Process engine = new ProcessBuilder(Settings.required("HW1_TESSERACT_BIN"),
                        input.toString(), "stdout", "--tessdata-dir", tessdata.toString(), "-l", "eng")
                        .redirectOutput(output.toFile()).redirectError(ProcessBuilder.Redirect.DISCARD).start();
                try {
                    if (!engine.waitFor(90, TimeUnit.SECONDS)) throw new IOException("OCR timed out");
                    if (engine.exitValue() != 0) throw new IOException("Tesseract failed with exit code " + engine.exitValue());
                    return Files.readString(output).strip();
                } finally {
                    if (engine.isAlive()) {
                        engine.destroyForcibly();
                        engine.waitFor();
                    }
                }
            } finally {
                Files.deleteIfExists(output);
            }
        } finally {
            Files.deleteIfExists(input);
        }
    }

    private static final class LocalOcr {
        private final Tesseract engine = new Tesseract();

        LocalOcr(Path tessdata) {
            engine.setDatapath(tessdata.toString());
            engine.setLanguage("eng");
        }

        String extractText(BufferedImage image) throws net.sourceforge.tess4j.TesseractException {
            return engine.doOCR(image).strip();
        }
    }
}
