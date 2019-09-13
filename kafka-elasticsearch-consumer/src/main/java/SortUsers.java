import java.util.Arrays;
import java.util.Comparator;

public class SortUsers {

    public static void main(String[] args)
    {
        // testing input
        // O(n log(n)) runtime complexity
        Integer[] testArray = {-2, 3, 5, 4, 10, 9, 42, 1, 2, 3};
        mergeSort(testArray, 0, testArray.length-1);
        for (Integer number : testArray
        ) {
            System.out.println(number);
        }
    }

    /**
     *
     * @param array
     * @param leftInd
     * @param midInd
     * @param rightInd
     */
    // merges two sub arrays of arr[]
    // first sub array is arr[l .. m]
    // second sub array is arr[m+1 .. r]
    private static void merge(Integer[] array, Integer leftInd, Integer midInd, Integer rightInd)
    {
        Integer sizeOfArray1 = midInd-leftInd+1;
        Integer sizeOfArray2 = rightInd-midInd;

        // create temp arrays
        Integer[] tempArray1 = new Integer[sizeOfArray1];
        Integer[] tempArray2 = new Integer[sizeOfArray2];

        // copy data to temp arrays
        for(Integer i = 0; i < sizeOfArray1; i++) {
            tempArray1[i] = array[leftInd+i];
        }
        for (Integer j = 0; j < sizeOfArray2; j++) {
            tempArray2[j] = array[midInd+1+j];
        }

        // merge temp arrays
        // initial indexes of first and second sub arrays
        Integer i = 0, j = 0;

        // now we are performing operations on the initial array we started off with
        // starting index of merged subarray array
        Integer k = leftInd;
        while (i < sizeOfArray1 && j < sizeOfArray2) {
            if (tempArray1[i] <= tempArray2[j]) {
                array[k] = tempArray1[i];
                i++;
            } else {
                array[k] = tempArray2[j];
                j++;
            }
            k++;
        }

        // clean up remaining members of the leftover array
        // note that only one of these while loops will be entered
        while (i < sizeOfArray1) {
            array[k] = tempArray1[i];
            i++;
            k++;
        }

        while (j < sizeOfArray2) {
            array[k] = tempArray2[j];
            j++;
            k++;
        }

    }

    /**
     *
     * @param array
     * @param leftInd
     * @param rightInd
     */
    public static void mergeSort(Integer[] array, Integer leftInd, Integer rightInd)
    {
        Integer midInd = (leftInd+rightInd)/2;

        if (leftInd < rightInd) {
            mergeSort(array, leftInd, midInd);
            mergeSort(array, midInd+1, rightInd);
            merge(array, leftInd, midInd, rightInd);
        }
    }

    /**
     *
     * @param arrayToBeSorted
     * @return
     */
    public static String[][] sort2DArrayByColumn(String[][] arrayToBeSorted, Integer index)
    {
        // leverage Java 8 streams to sort 2D string array, parsing supposed Integer column to type Integer
        String[][] out = Arrays.stream(arrayToBeSorted)
                .sorted(Comparator.comparing(x -> -Integer.parseInt(x[index])))
                .toArray(String[][]::new);
        return out;
    }
}