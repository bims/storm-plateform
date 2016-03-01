package kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import otherClass.MyConstants;

import java.util.Properties;
import java.util.Random;

public class MyProducer {

    public static final double ONE_MOVE = 1;

    public static void main(String[] args){
        long events = 1;// nombre de messages
        Random rnd = new Random();

        //Variables utilisees pour la simulation
        double x = 44.69;
        double y = 2.24;

        Properties props = new Properties();

        //Un broker suffit pour la premiere version
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        int j = 0;
        int i = 0;
        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 0; nEvents < events; nEvents++) {
            //On genere les deplacements
            if(rnd.nextBoolean()){
                x = x + ONE_MOVE;
            }
            else x = x - ONE_MOVE;
            if(rnd.nextBoolean()){
                y = y + ONE_MOVE;
            }
            else y = y - ONE_MOVE;

            String msg = x+" "+y;
            String key = ""; //Clé non nécessaire pour l'instant, obligatoire (?) de mettre une valeur pour utiliser Partitioner

            //Ici, on specifie le nom du topic dans lequel on va envoyer le message
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(MyConstants.TOPIC_NAME,key,msg);
           /* try {
                //A modifier ???
                //Thread.sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            producer.send(data);
            System.out.println("Message sent:" + msg);
        }
        producer.close();
    }
}

