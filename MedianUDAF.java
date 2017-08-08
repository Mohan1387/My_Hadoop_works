/*
* @author: Mohan Manivannan
* @Description: Hive UDAF to Find Median of a column 
*      name = "MedianUDAF",
*      value = "_FUNC_(double) - Computes Median",
*      extended = "select MedianUDAF(column_name) from table;"
*/

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import java.util.*;

@SuppressWarnings("deprecation")

public class MedianUDAF extends UDAF{
    static final Log LOG = LogFactory.getLog(MedianUDAF.class.getName());
 
    public static class MedianUDAFEvaluator implements UDAFEvaluator{
 
        /**
         * Use item class to serialize intermediate computation
         */
        public static class Item{
        	ArrayList<Double> list = new ArrayList<Double>();
            int cnt = 0;
        }
         
        
        private Item item = null;
         
        /**
         * function: Constructor
         */
        public MedianUDAFEvaluator(){
            super();
            init();
        }
         
        /**
         * function: init()
         * Its called before records pertaining to a new group are streamed
         */
        public void init() {
            LOG.debug("======== init ========");
            item = new Item();          
        }
         
        /**
         * function: iterate
         * This function is called for every individual record
         * @param value
         * @return
         * @throws HiveException 
         */
        public boolean iterate(double value) throws HiveException{
            LOG.debug("======== iterate ========");
            if(item == null)
                throw new HiveException("Item is not initialized");
            item.list.add(value);
            item.cnt = item.cnt + 1;
            return true;
        }
         
        /**
         * function: terminate
         * this function is called after the last record of the group has been streamed
         * @return
         */
        public double terminate(){
            LOG.debug("======== terminate ========");
            
            //sorting the final list to find median
            Collections.sort(item.list);
            
            //converting List to array
            Double[] fnlarray = new Double[item.list.size()];
            fnlarray = item.list.toArray(fnlarray);
            
            //index variables for array
            int ind = 0;
            int topvalue = 0;
            int bottomvalue = 0;
            
            //final result variable
            double returnvalue = 0;
            
            // to find Median check if the array is of even or odd count
            
            if(item.list.size() % 2 == 0){
            
            	//logic to find the middle two position in array where median falls for event count
            topvalue = Integer.parseInt(Math.round(item.list.size()*0.5)+"");
            bottomvalue = Integer.parseInt(Math.round((item.list.size()*0.5)+1)+"");
            
            // finding median with two middle position
            returnvalue = (fnlarray[topvalue]+fnlarray[bottomvalue])/2;
            
            }else{
            
            	//logic to find the middle position in array where median falls for odd count
            ind = Integer.parseInt(Math.round(((item.list.size()-1)*0.5)+1)+"");
            returnvalue = fnlarray[ind];
            }
            
            
            return returnvalue;
        }
         
        /**
         * function: terminatePartial
         * this function is called on the mapper side and 
         * returns partially collected list results. 
         * @return
         */
        public Item terminatePartial(){
            LOG.debug("======== terminatePartial ========");            
            return item;
        }
         
         
        /**
         * function: merge
         * This function is called two merge two partially collected lists results
         * @param another
         * @return
         */
        public boolean merge(Item another){
            LOG.debug("======== merge ========");           
            if(another == null) return true;
            item.list.addAll(another.list);
            item.cnt += another.cnt;
            return true;
        }
    }
}