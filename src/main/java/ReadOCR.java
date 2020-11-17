import com.asprise.ocr.Ocr;
import net.sourceforge.tess4j.OCRResult;
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
    //set language to english (default)
    //enable HOCR output to maintain the same structure as the scanned image (even though it doesn't work correctly)
    private static Tesseract getTess(){
        Tesseract reader = new Tesseract();
        reader.setDatapath("C:\\Users\\ibrahim\\Desktop\\tessdata") ;
//        reader.setLanguage("eng");
        reader.setHocr(true);
        return reader;
    }
    private static Ocr getAspriseOcr(){
        Ocr.setUp();
        Ocr reader = new Ocr();
        reader.startEngine("eng", Ocr.SPEED_FASTEST);
        return reader;
    }

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
            System.out.println(OCR_result);
            //open the result.html file
            Path file = Paths.get("C:\\Users\\ibrahim\\Desktop\\tessimages\\resultTess.html");
            //write both image and OCRed text into result.html file
            String[] to_write = {"<img src=" + image_url + ">", OCR_result};
            Files.write(file, Arrays.asList(to_write));
        } catch (TesseractException | IOException e) {
            e.printStackTrace();
        }

//        //ASPRISE PART
//        //create my OCR reader
//        Ocr myreader = getAspriseOcr();
//        String res = myreader.recognize(new URL[]{url},
//                Ocr.RECOGNIZE_TYPE_TEXT,
//                Ocr.OUTPUT_FORMAT_XML);
//        Path file = Paths.get("C:\\Users\\ibrahim\\Desktop\\tessimages\\resultAsprise.html");
//        //write both image and OCRed text into result.html file
//        String[] to_write = {"<img src=" + image_url + ">","<div>"+ res+"</div>"};
//        try {
//            Files.write(file, Arrays.asList(to_write));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        myreader.stopEngine();

    }
}
