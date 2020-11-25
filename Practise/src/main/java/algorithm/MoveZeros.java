package algorithm;

/**
* @Description:
 * 给定一个数组nums，编写一个函数将所有0移动到数组的末尾，同时保持非零元素的相对顺序。
 *
 * 示例:
 *
 * 输入: [0,1,0,3,12]
 * 输出: [1,3,12,0,0]
 * 说明:
 *
 * 必须在原数组上操作，不能拷贝额外的数组。
 * 尽量减少操作次数。
* @Param:
* @return:
* @Author: jian.tan
* @Date: 2020/11/23
*/
public class MoveZeros {
    /**
     * 类双指针模式
     * 下标i和j
     * 1：当j下标代表的值不为0时，将num[j]的值跟num[i]的值互换
     * 3：i下标自增+1
     */
    public void moveZeros(int[] nums){
        int i=0;
        for (int j=0;j<nums.length;j++){
            //当j下标
            if (nums[j]!=0){
                int temp=nums[i];
                nums[i] = nums[j];
                nums[j] = temp;
                i++;
            }
        }
    }
}
