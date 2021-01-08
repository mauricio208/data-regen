rcsv = require('./readCsvToJson');

const LABELS = { 
  over18 : [
  '"BLOCK_2392ed35-b006-48a9-878f-215337ea20e3"',
  '"COMPONENT_94b18e5b_3057_427b_abd7_b57e67cf77b5"',
  '"BLOCK_e74f57dc-dfdb-4699-98f8-80936222792e"',
  '"COMPONENT_07a01d99_5191_449a_81f8_a28b39c0f426"'
],
 videoWatched : [
  '"BLOCK_80b63e02-acc5-43b2-803d-f65404294883"',
  '"COMPONENT_cf7b1c90_a31f_4232_8526_77655f20fea9"',
  '"BLOCK_f9f235c2-ed45-435a-8836-80779172b76b"',
  '"COMPONENT_2353ccd0_7ae8_4805_b463_45d263c8d3b5"',
  '"BLOCK_6e67d81e-9afc-48bd-9383-1816cb9c90c9"',
  '"COMPONENT_7fcbf3a3_12bb_45c7_a837_43a09f247baf"',
  '"BLOCK_5e9e4930-03c5-48b2-ac5b-6637c2c304ea"',
  '"COMPONENT_8c29eb1f_dd78_4872_8295_e45b04b7c04a"',
  '"BLOCK_0afbf3f8-6af2-4d52-84aa-bffc0cc63373"',
  '"COMPONENT_13bbb05c_32d8_42c9_8753_7a98ab959bb8"',
  '"BLOCK_6d470a75-dbd2-49b7-9b23-e61da1549ddb"',
  '"COMPONENT_07933844_4010_4e65_84c0_9ccc1bb9d7a9"',
  '"BLOCK_6b0f87b0-313f-492a-a6d4-a6a5a8085cbd"',
  '"COMPONENT_4f559cf9_b187_481e_b20a_1cbd42626fc3"',
  '"BLOCK_f6dc785d-d673-4e87-acd8-59ed069c391e"',
  '"COMPONENT_3c339534_3fc6_4641_86a9_a3a18c8c5ba4"',
  '"BLOCK_585ef111-ed6e-4552-b04b-a698482f0206"',
  '"COMPONENT_d0a30315_aed4_47eb_b185_eb5fb041acc9"',
  '"BLOCK_dbb59f47-2d43-4bb8-b162-ccdaa3ac0066"',
  '"COMPONENT_881b8c4b_20b4_401e_89e1_f0e032e09063"',
  '"BLOCK_9c4a99ec-319d-4d69-855d-8831414bbfd6"',
  '"COMPONENT_5df8dec0_b5f4_4f1f_b8a1_e4b57ffabaf4"',
  '"BLOCK_64a66d99-ddcd-44ab-ad85-3235a6702832"',
  '"COMPONENT_b3c9a39b_b521_4f5d_bc1d_b7619700924f"',
  '"BLOCK_21328110-38ff-4341-b0fb-4f91cdd2a9a3"',
  '"COMPONENT_cb40da7f_b301_4049_b15a_f728cbbd7982"',
  '"BLOCK_86413811-668f-474b-9f59-f9347c28f320"',
  '"COMPONENT_cb14372e_3ea2_4a35_bafe_d9e2b64f7d29"'
],
 someoneElse : [
  '"BLOCK_070b1ca6-c3a1-4d15-a9bb-f5a005e22936"',
  '"COMPONENT_78e3b336_d67d_4c5d_a4a2_e041f2c2ac86"'
],

//corrupted Data example
// {
//   block: '"BLOCK_ffd2fb5f-4dc7-41b*d-81f635dd47f3"',
//   session_id: '34050',
//   user_id: '30635',
//   'Date(createdAt)': '2020-05-11',
//   'date(updatedAt)': '2020-05-11'
// },

 contactAdvisor : [
  '"BLOCK_ffd2fb5f-4dc7-41b2-974d-81f635dd47f3"',
  '"COMPONENT_7288340e_f2d0_46b2_8f97_b64cb16c2572"'
]}

function groupByUserIdSessionId(entries) {
  let entriesByUserId = {};
  for (const entrie of entries) {
    if (entriesByUserId[entrie.user_id]) {
      if (entriesByUserId[entrie.user_id][entrie.session_id]) {
        entriesByUserId[entrie.user_id][entrie.session_id].push(entrie)
      }else{
        entriesByUserId[entrie.user_id][entrie.session_id] = [entrie]
      }
    }else{
      entriesByUserId[entrie.user_id] = {}
      entriesByUserId[entrie.user_id][entrie.session_id] = [entrie]
    }
  }
  return entriesByUserId;
}

function createKPIObject(entrie) {
  for (const label of Object.keys(LABELS)) {
    if (LABELS[label].indexOf(entrie.block) > -1) {
      let labelObject = {}
      labelObject[label] = 1;
      return {
        "session_id": entrie.session_id,
        "user_id": entrie.user_id,
        "labels": labelObject,
        "updatedAt": entrie["date(updatedAt)"],
        "createdAt": entrie["Date(createdAt)"]
      }
    }
  }
}

function createKPIObjectForSemiCorruptedData(entrie, matchMultiple) {
  for (const label of Object.keys(LABELS)) {
    let match = matchMultiple?'.{0,5}':'.';
    let corruptedBlockRegex = new RegExp(entrie.block.replace('*',match))
    if (LABELS[label].some(l=>corruptedBlockRegex.test(l))) {
      let labelObject = {}
      labelObject[label] = 1;
      return {
        // "block_id": entrie.block,
        "session_id": entrie.session_id,
        "user_id": entrie.user_id,
        "labels": labelObject,
        "updatedAt": entrie["date(updatedAt)"],
        "createdAt": entrie["Date(createdAt)"]
      }
    }
  }
}

async function getPowerBiData() {
  let blocks = await rcsv('blocks.csv');
  let regexExp = /.*\*.*/g;
  let regexExpRecuperable = /^[^\*]*\*[^\*]*$/g;
  let corruptedEntries = blocks.filter(row=>regexExp.test(row.block))
  let recuperableEntries =  blocks.filter(row=>regexExpRecuperable.test(row.block))
  let entries = blocks.filter(row=>!regexExp.test(row.block))
  let powerBiData = [];
  for (const entrie of entries) {
    let kpiObject = createKPIObject(entrie);
    if (kpiObject) {
      powerBiData.push(kpiObject)
    }
  }
  let powerBiDataRec=[];
  for (const entrie of rcen) {
    let kpiObject = createKPIObjectForSemiCorruptedData(entrie, true);
    if (kpiObject) {
      powerBiDataRec.push(kpiObject)
    }
  }
  return {powerBiData, powerBiDataRec}
}

let gr;
getPowerBiData.then(r=>{gr=r})

