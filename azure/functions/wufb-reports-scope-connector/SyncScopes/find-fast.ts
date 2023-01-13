export default function findFast(arr: unknown[], searchValue: unknown) {

  let start = 0, end = arr.length - 1;

  // Iterate to end
  while (start <= end) {
    // Find the mid index
    let mid = Math.floor((start + end) / 2);

    // If element is present at mid, return true
    if (arr[mid] === searchValue)
      return true;
    // Else look in left or right half accordingly
    else if (arr[mid] < searchValue)
      start = mid + 1;
    else
      end = mid - 1;
  }

  return false;
}