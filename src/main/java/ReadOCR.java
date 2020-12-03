import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import net.sourceforge.tess4j.util.LoadLibs;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class ReadOCR {

    //Create an OCR reader using Tesseract package
    //set training data to improve OCR accuracy
    private static Tesseract getTess(){
        Tesseract reader = new Tesseract();
         File tessDataFolder = LoadLibs.extractTessResources("tessdata");
         reader.setDatapath(tessDataFolder.getPath());
        return reader;
    }

    public static String main(String[] args) {

        String image_url=args[1];
        String output=image_url+"$";

        //download image from URL

        URL url = null;
        try {
            url = new URL(image_url);
        } catch (MalformedURLException e) {
            output+=e.getMessage();
        }

        BufferedImage img = null;
        try {
            img = ImageIO.read(url);
        } catch (IOException e) {
            output+=e.getMessage();
        }

        //TESSERACT PART
        //create my OCR reader
        Tesseract my_reader = getTess();
        //perform OCR on image_to_read and write into HTML file result.html
        try {
            //perform OCR
            String OCR_result = my_reader.doOCR(img);
            output+=OCR_result;
        } catch (TesseractException e) {
            output+=e.getMessage();
        }
        return output;
    }
}

