import { Ripgrep } from "../packages/opencode/src/file/ripgrep"

async function test() {
  console.log("Testing file scanning with node_modules exclusion...")
  
  const testDir = `${import.meta.dir}/basic`
  let fileCount = 0
  let nodeModulesCount = 0
  
  for await (const file of Ripgrep.files({ cwd: testDir })) {
    fileCount++
    if (file.includes("node_modules")) {
      nodeModulesCount++
    }
  }
  
  console.log(`Total files scanned: ${fileCount}`)
  console.log(`Files from node_modules: ${nodeModulesCount}`)
  
  if (nodeModulesCount > 0) {
    console.error("FAIL: node_modules was not excluded!")
    process.exit(1)
  }
  
  console.log("PASS: node_modules was properly excluded!")
  process.exit(0)
}

test().catch((err) => {
  console.error("Test failed:", err)
  process.exit(1)
})
