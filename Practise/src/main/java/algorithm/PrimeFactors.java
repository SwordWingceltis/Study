package algorithm;

/**
* @Description: 最大质因数
* 13195的所有质因数为5、7、13和29。 600851475143最大的质因数是多少？
* @Param:
* @return:
* @Author: jian.tan
* @Date: 2020/11/24
*/
public class PrimeFactors {
    public long primeFactors(long numbers){
        for(int i=2;i*i<numbers+1;i++)
        {
            if(numbers%i==0)
            {
                return primeFactors(numbers/i)>primeFactors(i)?primeFactors(numbers/i):primeFactors(i);
            }
        }
        return numbers;// 如果找不到因数他自己就是最大素因数
    }
}
