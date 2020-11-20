import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
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
        reader.setDatapath("C:\\Users\\ibrahim\\Desktop\\tessdata") ;
        return reader;
    }

    //todo: maybe change return value to String? maybe instead of printStackTrace return the error?
    // we need to return the OCR_result or <a short description of the exception>

    public static void main(String[] args) {
        //download image from URL
        String image_url = "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif";
        URL url = null;
        try {
            url = new URL(image_url);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        BufferedImage img = null;
        try {
            img = ImageIO.read(url);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //TESSERACT PART
        //create my OCR reader
        Tesseract my_reader = getTess();
        //perform OCR on image_to_read and write into HTML file result.html
        try {
            //perform OCR
            String OCR_result = my_reader.doOCR(img);

            //todo: delete print
            System.out.println(OCR_result);

            //open the result.html file
            Path file = Paths.get("C:\\Users\\ibrahim\\Desktop\\tessimages\\resultTess.html");
            //write both image and OCRed text into result.html file
            String[] to_write = {"<img src=" + image_url + ">", OCR_result};
            Files.write(file, Arrays.asList(to_write));
        } catch (TesseractException | IOException e) {
            e.printStackTrace();
        }
    }
}

