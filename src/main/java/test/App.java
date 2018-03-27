package test;

import  com.zyuc.stat.utils.DateUtils;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        String t = DateUtils.timeCalcWithFormatConvertSafe("20170911","yyyyMMdd", 1*24*3600, "yyyyMMdd");
        System.out.println("args = [" + t + "]");
    }


}
