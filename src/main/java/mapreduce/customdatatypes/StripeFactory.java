package mapreduce.customdatatypes;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

public abstract class StripeFactory<K, V> {

	protected StripeFactory() {
	}

	protected static StripeFactory instance;
	
	public static StripeFactory getInstance(){
		return instance;
	}

	abstract public  Stripe<K, V> createNewStripe();

	abstract public V combineValues(V v1, V v2);

    public Stripe<K, V> sumOverStripes(Iterator<Stripe<K, V>> values) {
		Stripe<K, V> resultStripe = createNewStripe();
		if (values.hasNext()) {
			do {
				Set<Entry<K, V>> stripeEntries = values.next().getMap()
						.entrySet();
				for (Entry<K, V> e : stripeEntries) {
					K key = e.getKey();
					V val = e.getValue();
					if (resultStripe.map.containsKey(key))
						resultStripe.map.put(key,
								combineValues(resultStripe.map.get(key), val));
					else
						resultStripe.map.put(key, val);
				}
			} while (values.hasNext());
		}
		return resultStripe;
	}
}
