package kafka;


//define server constants
public final class Consts {
	public static int port = 9092;
	public static String[] topics = {"FirstTopic", "SecondTopic"};
	public static String[] keys = {"Key1", "Key2"};
	public static String[] values = {"Hello", "World"};
	

	private Consts(){
		throw new AssertionError();
	}

}
