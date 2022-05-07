import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

public class WebReader {

  public static void main(String[] args) {

    try {

      URL url = new URL("https://www.udemy.com/featured-topics/");

      // read text returned by server
      BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

      String line;
      StringBuilder builder = new StringBuilder();
      while ((line = in.readLine()) != null) {
        if (line.contains("/topic/")) {
          builder.append("(")
              .append("email_")
              .append(line.split("/")[2])
              .append(")|");
        }
      }
      System.out.println(builder.toString());
      in.close();

    } catch (MalformedURLException e) {
      System.out.println("Malformed URL: " + e.getMessage());
    } catch (IOException e) {
      System.out.println("I/O Error: " + e.getMessage());
    }

  }

}