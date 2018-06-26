package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class PageKey implements WritableComparable<PageKey> {
	String pageTo;
	String pageFrom;

	public PageKey() {
	}

	public PageKey(String pageTo, String pageFrom) {
		super();
		this.pageTo = pageTo;
		this.pageFrom = pageFrom;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pageTo = WritableUtils.readString(in);
		pageFrom = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, pageTo);
		WritableUtils.writeString(out, pageFrom);
	}

	@Override
	public int compareTo(PageKey o) {
		int cmp = pageTo.compareTo(o.pageTo);
		if(cmp == 0) 
			cmp = pageFrom.compareTo(o.pageFrom);
		return cmp;
	}

	@Override
	public String toString() {
		return  pageTo+"|"+pageFrom;
	}
	
	

}
