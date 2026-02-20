import { DefaultArtifactClient } from '@actions/artifact';

const artifactClient = new DefaultArtifactClient();

let path = process.argv[2];
let rootDir = process.argv[3];
let name = process.argv[4];
await artifactClient.uploadArtifact(name, [path], rootDir);
