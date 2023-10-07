import { getCsv } from 'k6/x/large-csv';
const c = getCsv("test.csv")
export default function () {
  let vars = c.getLine(",")
  console.log(`${vars[0]} ${vars[1]}`);
}
export function teardown(data) {
  c.close()
}
