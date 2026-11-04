// Helpers shared by the jsonnet scenarios, the way projects keep a library
// next to their .woodpecker/*.jsonnet.
{
  // step is a dummy-backend step running commands.
  step(name, commands, extra={}):: {
    name: name,
    image: 'dummy',
    commands: commands,
  } + extra,

  // workflow wraps steps; clone is skipped, the dummy backend has no repo.
  workflow(steps, depends_on=[]):: {
    skip_clone: true,
    steps: steps,
  } + (if depends_on == [] then {} else { depends_on: depends_on }),
}
