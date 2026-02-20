const { DefaultArtifactClient } = require('@actions/artifact');

const artifactClient = new DefaultArtifactClient();

let path = process.argv[2];
let name = process.argv[3];
await artifactClient.uploadArtifact(name, [path], ".");
