package com.dataflow.utils;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class Collector<T> implements Iterable<T> {
	
	private final T buffer[];
	private final AtomicLong pointer = new AtomicLong();
	private final static int SIZE=10000;
	
	private static class CollectorIterator<T> implements Iterator<T>{

		private Collector<T> collect;
		private int current = 0;
		
		public  CollectorIterator(Collector<T> collect) {
			this.collect = collect;
		}
		
		@Override
		public boolean hasNext() {
			return current < collect.pointer.intValue();
		}

		@Override
		public T next() {
			return collect.buffer[current++];
		}
	}
	
	public Collector(){
		buffer = (T[]) new Object[SIZE];
	}
	
	public void add(T element){
		buffer[(int)pointer.getAndIncrement() % buffer.length] = element;
	}
	
	//TODO: Write to disk if 75% complete
	public boolean snapshot(){
		return true;
	}
	
	public long getSize(){
		return pointer.get();
	}

	
	@Override
	public Iterator<T> iterator() {
		return new CollectorIterator(this);
	}
	
	//TODO: Change this..
	@Override
	public String toString(){
		return "Collector";
	}
	
}
