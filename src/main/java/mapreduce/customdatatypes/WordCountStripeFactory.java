package mapreduce.customdatatypes;

public class WordCountStripeFactory extends StripeFactory<String, Integer> {

	static {
		instance = new WordCountStripeFactory();
	}

	protected WordCountStripeFactory() {
	}

    public Stripe<String, Integer> createNewStripe(){
    	return new WordCountMap();
    }

    public Integer combineValues(Integer v1, Integer v2){
		return v1 + v2;
    }
    
}
