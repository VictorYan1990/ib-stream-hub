# Skills (canonical)

This directory is the **source of truth** for reusable Agent Skills, following
the vendor-neutral Agent Skills standard. Tool-specific paths symlink here:

- `.cursor/skills` → `../.agents/skills`
- `.claude/skills` → `../.agents/skills`

## Adding a skill

Create one subdirectory per skill, each containing a `SKILL.md` with YAML
frontmatter (`name`, `description`) and markdown instructions:

```
.agents/skills/
└── my-skill/
    └── SKILL.md
```

Agents load the `description` at session start and pull in the full body on
demand when a task matches. Edit skills here — never edit the symlinked copies.
