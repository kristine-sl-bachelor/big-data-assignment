package xml;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

public class XmlStringParser {

    Document doc;

    public XmlStringParser( String xml ) {

        try {

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = null;
            builder = factory.newDocumentBuilder();

            InputSource input = new InputSource();
            input.setCharacterStream( new StringReader( xml ) );
            doc = builder.parse( input );

        } catch ( ParserConfigurationException e ) {

            e.printStackTrace();

        } catch ( SAXException e ) {

            e.printStackTrace();

        } catch ( IOException e ) {

            e.printStackTrace();
        }

    }

    public String getValueFromTag( String tag ){

        Node node = doc.getElementsByTagName( tag ).item( 0 );

        if( node != null ) return node.getTextContent();

        return "";
    }
}
