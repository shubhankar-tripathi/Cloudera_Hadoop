package extracredit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringPairWritable implements WritableComparable<StringPairWritable> {

  String left;
  String right;

  public StringPairWritable()
  {

  }

  public StringPairWritable(String left, String right) {
    this.left = left;
    this.right = right;    
  }


  public void write(DataOutput out) throws IOException
  {
	  out.writeUTF(this.left);
	  out.writeUTF(this.right);
  }

 
  public void readFields(DataInput in) throws IOException
  {
	  this.left = in.readUTF();
	  this.right = in.readUTF();
  }

  
  public int compareTo(StringPairWritable other)
  {
	  int ret = this.left.compareTo(other.left);
	  if(ret==0)
	  {
    	ret = this.right.compareTo(other.right);
	  }
	  return ret;
  }

  public String toString() {
     return "(" + left + "," + right + ")";
  }

  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    StringPairWritable other = (StringPairWritable) obj;
    if (left == null) {
      if (other.left != null)
        return false;
    } else if (!left.equals(other.left))
      return false;
    if (right == null) {
      if (other.right != null)
        return false;
    } else if (!right.equals(other.right))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((left == null) ? 0 : left.hashCode());
    result = prime * result + ((right == null) ? 0 : right.hashCode());
    return result;
  }
}
