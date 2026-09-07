//! The screen effects: digital rain, the CRT overlay (scanlines, sweep,
//! vignette, flicker) and the cold-boot sequence.
//!
//! All of it is painted with [`egui::Painter`] primitives -- no shader, no
//! extra dependency, nothing that could pull a system library into a build
//! that is deliberately free of one (see the zero-OpenSSL note in
//! `CLAUDE.md`). Randomness comes from the xorshift below rather than `rand`
//! for the same reason: one more crate for jitter is not worth it.
//!
//! Everything here is decoration and must stay cheap: the window sits open for
//! hours while a stream runs, so the whole overlay is O(screen height / 3) line
//! segments and the rain is a few hundred cached single-character galleys.

use egui::{Color32, FontId, Pos2, Rect, Rounding, Stroke, Vec2};

use crate::theme::{self, CELL};

/// xorshift64*. Deterministic, seedable, and about ten lines -- all this needs
/// is jitter that does not repeat visibly.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // Zero is the one state xorshift cannot leave.
        Self(seed | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `[0, 1)`.
    pub fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u32 << 24) as f32
    }

    pub fn range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + self.next_f32() * (hi - lo)
    }

    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }
}

/// The glyphs the rain is made of. ASCII only, on purpose -- katakana would be
/// the obvious choice and is exactly the thing the bundled font does not
/// promise to have.
const RAIN_GLYPHS: &[u8] = b"01234567890ABCDEF#$%&*+=<>/\\|_-[]{}!?";

const RAIN_COLUMN_SPACING: f32 = 14.0;
const RAIN_TRAIL: usize = 12;

struct RainColumn {
    /// Y of the leading glyph, in points, allowed to run past the bottom so
    /// the column restarts off-screen instead of popping into view.
    head: f32,
    /// Points per second.
    speed: f32,
    /// Per-column glyph ring, re-rolled as it falls so the trail shimmers.
    glyphs: [u8; RAIN_TRAIL],
    /// Overall brightness of this column; most stay faint, a few burn.
    intensity: f32,
}

/// Falling-glyph background. Faint by design: it lives *behind* the panels and
/// is only really visible in the margins, where it turns dead space into
/// something that says "this box is doing work" without competing with the
/// numbers that matter.
pub struct MatrixRain {
    columns: Vec<RainColumn>,
    rng: Rng,
    laid_out_for: Vec2,
    /// Advanced with wall time so the shimmer is frame-rate independent.
    shimmer: f32,
}

impl MatrixRain {
    pub fn new(seed: u64) -> Self {
        Self {
            columns: Vec::new(),
            rng: Rng::new(seed),
            laid_out_for: Vec2::ZERO,
            shimmer: 0.0,
        }
    }

    fn relayout(&mut self, size: Vec2) {
        let count = (size.x / RAIN_COLUMN_SPACING).ceil().max(1.0) as usize;
        self.columns.clear();
        for _ in 0..count {
            let head = self.rng.range(-size.y, size.y);
            let speed = self.rng.range(28.0, 96.0);
            let intensity = self.rng.range(0.25, 1.0);
            let mut glyphs = [b' '; RAIN_TRAIL];
            for slot in glyphs.iter_mut() {
                *slot = RAIN_GLYPHS[self.rng.below(RAIN_GLYPHS.len())];
            }
            self.columns.push(RainColumn {
                head,
                speed,
                glyphs,
                intensity,
            });
        }
        self.laid_out_for = size;
    }

    /// Steps and paints one frame. `dt` is the frame's own delta, so the rain
    /// falls at the same rate whether the window is repainting at 60 Hz or has
    /// been throttled in the background.
    pub fn paint(&mut self, painter: &egui::Painter, rect: Rect, dt: f32, opacity: f32) {
        if rect.width() < 1.0 || rect.height() < 1.0 {
            return;
        }
        if (self.laid_out_for - rect.size()).abs().max_elem() > RAIN_COLUMN_SPACING {
            self.relayout(rect.size());
        }

        self.shimmer += dt;
        let reroll = self.shimmer > 0.06;
        if reroll {
            self.shimmer = 0.0;
        }

        let font = FontId::monospace(12.0);
        let row = CELL * 4.0;
        // Rolled outside the loop so the borrow of `self.rng` never overlaps
        // the mutable walk over `self.columns`.
        let mut rolls = [0usize; 4];
        for slot in rolls.iter_mut() {
            *slot = self.rng.below(RAIN_GLYPHS.len());
        }
        let mut jitter = self.rng.below(RAIN_TRAIL);

        for (index, column) in self.columns.iter_mut().enumerate() {
            column.head += column.speed * dt;
            if column.head - (RAIN_TRAIL as f32 * row) > rect.height() {
                column.head = -row * RAIN_TRAIL as f32;
            }
            if reroll {
                jitter = (jitter + 1) % RAIN_TRAIL;
                column.glyphs[jitter] = RAIN_GLYPHS[rolls[index % rolls.len()]];
            }

            let x = rect.left() + index as f32 * RAIN_COLUMN_SPACING + 3.0;
            for step in 0..RAIN_TRAIL {
                let y = column.head - step as f32 * row;
                if y < rect.top() - row || y > rect.bottom() {
                    continue;
                }
                // The head glyph is the freshly struck phosphor; everything
                // behind it is the decay trail.
                let fade = 1.0 - step as f32 / RAIN_TRAIL as f32;
                let alpha = (fade * fade * column.intensity * opacity * 150.0) as u8;
                if alpha < 3 {
                    continue;
                }
                let color = if step == 0 {
                    Color32::from_rgba_unmultiplied(190, 255, 210, alpha.saturating_add(70))
                } else {
                    Color32::from_rgba_unmultiplied(
                        theme::PHOSPHOR.r(),
                        theme::PHOSPHOR.g(),
                        theme::PHOSPHOR.b(),
                        alpha,
                    )
                };
                painter.text(
                    egui::pos2(x, y),
                    egui::Align2::LEFT_TOP,
                    (column.glyphs[step] as char).to_string(),
                    font.clone(),
                    color,
                );
            }
        }
    }
}

/// Scanlines + a slow refresh sweep + vignette + mains flicker, painted over
/// the finished frame.
///
/// Order matters: this goes in a foreground layer so it sits on top of *every*
/// widget. Half-applied (behind the panels) it just looks like a dirty
/// background; over the top it reads as glass.
pub fn crt_overlay(ctx: &egui::Context, rect: Rect, time: f64) {
    let painter = ctx.layer_painter(egui::LayerId::new(
        egui::Order::Foreground,
        egui::Id::new("crt_overlay"),
    ));

    // Scanlines. One dark line every `CELL` points; the gaps are the "lit"
    // rows. Alpha is low enough to read as texture rather than as blinds.
    let line = Stroke::new(1.0, Color32::from_rgba_unmultiplied(0, 0, 0, 46));
    let mut y = rect.top();
    while y < rect.bottom() {
        painter.line_segment(
            [egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)],
            line,
        );
        y += CELL;
    }

    // The refresh sweep: a soft bright band travelling down the tube every
    // few seconds, the way a phone camera catches a CRT mid-refresh.
    let period = 7.0;
    let sweep_y = rect.top() + ((time % period) / period) as f32 * rect.height();
    let band = 44.0;
    let steps = 10;
    for step in 0..steps {
        let t = step as f32 / steps as f32;
        let alpha = ((1.0 - t) * 9.0) as u8;
        if alpha == 0 {
            continue;
        }
        let offset = t * band;
        painter.rect_filled(
            Rect::from_min_max(
                egui::pos2(rect.left(), sweep_y + offset),
                egui::pos2(rect.right(), sweep_y + offset + band / steps as f32 + 1.0),
            ),
            Rounding::ZERO,
            Color32::from_rgba_unmultiplied(
                theme::PHOSPHOR.r(),
                theme::PHOSPHOR.g(),
                theme::PHOSPHOR.b(),
                alpha,
            ),
        );
    }

    vignette(&painter, rect);
}

/// Darkened edges, built as a gradient mesh rather than stacked rectangles so
/// there is no visible banding on a large window.
fn vignette(painter: &egui::Painter, rect: Rect) {
    let depth = (rect.width().min(rect.height()) * 0.22).clamp(24.0, 110.0);
    let edge = Color32::from_rgba_unmultiplied(0, 0, 0, 120);
    let clear = Color32::TRANSPARENT;

    let mut mesh = egui::Mesh::default();
    let mut band = |outer: [Pos2; 2], inner: [Pos2; 2]| {
        let base = mesh.vertices.len() as u32;
        mesh.colored_vertex(outer[0], edge);
        mesh.colored_vertex(outer[1], edge);
        mesh.colored_vertex(inner[1], clear);
        mesh.colored_vertex(inner[0], clear);
        mesh.add_triangle(base, base + 1, base + 2);
        mesh.add_triangle(base, base + 2, base + 3);
    };

    band(
        [rect.left_top(), rect.right_top()],
        [
            rect.left_top() + Vec2::new(0.0, depth),
            rect.right_top() + Vec2::new(0.0, depth),
        ],
    );
    band(
        [rect.left_bottom(), rect.right_bottom()],
        [
            rect.left_bottom() - Vec2::new(0.0, depth),
            rect.right_bottom() - Vec2::new(0.0, depth),
        ],
    );
    band(
        [rect.left_top(), rect.left_bottom()],
        [
            rect.left_top() + Vec2::new(depth, 0.0),
            rect.left_bottom() + Vec2::new(depth, 0.0),
        ],
    );
    band(
        [rect.right_top(), rect.right_bottom()],
        [
            rect.right_top() - Vec2::new(depth, 0.0),
            rect.right_bottom() - Vec2::new(depth, 0.0),
        ],
    );

    painter.add(egui::Shape::mesh(mesh));
}

/// Mains hum: a sub-percent brightness wobble on two incommensurable
/// frequencies, so it never settles into a visible pulse.
pub fn flicker(time: f64) -> f32 {
    let t = time as f32;
    1.0 + (t * 11.0).sin() * 0.015 + (t * 27.3).sin() * 0.008
}

/// True for the "on" half of a terminal cursor blink.
pub fn cursor_on(time: f64) -> bool {
    (time % 1.06) < 0.62
}

/// One line of the cold-boot log: the text, and the colour it prints in.
struct BootLine {
    text: &'static str,
    accent: Option<Color32>,
}

const BOOT_LINES: &[BootLine] = &[
    BootLine {
        text: "NOVASTREAM BIOS  --  local torrent gateway",
        accent: Some(theme::PHOSPHOR),
    },
    BootLine {
        text: "[  OK  ] cpu: detecting cores",
        accent: None,
    },
    BootLine {
        text: "[  OK  ] mounting cache volume",
        accent: None,
    },
    BootLine {
        text: "[  OK  ] rustls: certificate chain",
        accent: None,
    },
    BootLine {
        text: "[  OK  ] probing lan interfaces",
        accent: None,
    },
    BootLine {
        text: "[  OK  ] stremio addon protocol",
        accent: None,
    },
    BootLine {
        text: "[  OK  ] arming torrent engine",
        accent: None,
    },
    BootLine {
        text: "[ WARN ] gateway offline -- awaiting operator",
        accent: Some(theme::AMBER),
    },
    BootLine {
        text: "ready.",
        accent: Some(theme::PHOSPHOR),
    },
];

/// Characters printed per second while the boot log types itself out.
const BOOT_CPS: f64 = 320.0;
/// How long the finished log sits before it fades into the dashboard.
const BOOT_HOLD: f64 = 0.45;
const BOOT_FADE: f64 = 0.45;

/// The cold-boot screen.
///
/// It is a *cover*, not a gate: the dashboard is built and polling underneath
/// from the first frame, so nothing is delayed by it and a click skips
/// straight through. Anything that made the user wait to reach the start
/// button would be a worse app with a better intro.
#[derive(Default)]
pub struct BootSequence {
    started: Option<f64>,
    skipped: bool,
}

impl BootSequence {
    pub fn new() -> Self {
        Self {
            started: None,
            skipped: false,
        }
    }

    fn total_chars() -> f64 {
        BOOT_LINES.iter().map(|l| l.text.len() as f64).sum()
    }

    pub fn skip(&mut self) {
        self.skipped = true;
    }

    /// Paints the overlay and reports whether it is still showing.
    pub fn paint(&mut self, ctx: &egui::Context, rect: Rect, time: f64) -> bool {
        if self.skipped {
            return false;
        }
        let started = *self.started.get_or_insert(time);
        let elapsed = time - started;
        let typing = Self::total_chars() / BOOT_CPS;

        let fade = if elapsed < typing + BOOT_HOLD {
            1.0
        } else {
            1.0 - ((elapsed - typing - BOOT_HOLD) / BOOT_FADE).clamp(0.0, 1.0)
        };
        if fade <= 0.0 {
            self.skipped = true;
            return false;
        }

        let painter = ctx.layer_painter(egui::LayerId::new(
            egui::Order::Tooltip,
            egui::Id::new("boot_overlay"),
        ));
        let alpha = |color: Color32, scale: f32| {
            Color32::from_rgba_unmultiplied(
                color.r(),
                color.g(),
                color.b(),
                (color.a() as f32 * scale * fade as f32) as u8,
            )
        };

        painter.rect_filled(rect, Rounding::ZERO, alpha(theme::BG_VOID, 1.0));

        let font = FontId::monospace(12.0);
        let mut budget = elapsed * BOOT_CPS;
        let mut y = rect.top() + 28.0;
        let x = rect.left() + 20.0;
        let mut caret = None;

        for line in BOOT_LINES {
            if budget <= 0.0 {
                break;
            }
            let shown = (budget as usize).min(line.text.len());
            let text = &line.text[..shown];
            budget -= line.text.len() as f64;

            let color = line.accent.unwrap_or(theme::TEXT);
            // "[  OK  ]" prints in phosphor even on an otherwise plain line --
            // it is the only part of a boot log anyone actually scans.
            if line.accent.is_none() && shown >= 8 {
                let tag = theme::glow_text(
                    &painter,
                    egui::pos2(x, y),
                    egui::Align2::LEFT_TOP,
                    &text[..8],
                    font.clone(),
                    alpha(theme::PHOSPHOR, 1.0),
                );
                let rest = painter.text(
                    egui::pos2(tag.right(), y),
                    egui::Align2::LEFT_TOP,
                    &text[8..],
                    font.clone(),
                    alpha(theme::TEXT, 1.0),
                );
                caret = Some(egui::pos2(rest.right(), y));
                y += 16.0;
                continue;
            }

            let drawn = theme::glow_text(
                &painter,
                egui::pos2(x, y),
                egui::Align2::LEFT_TOP,
                text,
                font.clone(),
                alpha(color, 1.0),
            );
            caret = Some(egui::pos2(drawn.right(), y));
            y += 16.0;
        }

        if let Some(caret) = caret {
            if cursor_on(time) {
                painter.rect_filled(
                    Rect::from_min_size(caret + Vec2::new(2.0, 1.0), Vec2::new(7.0, 13.0)),
                    Rounding::ZERO,
                    alpha(theme::PHOSPHOR, 0.85),
                );
            }
        }

        painter.text(
            rect.center_bottom() - Vec2::new(0.0, 18.0),
            egui::Align2::CENTER_BOTTOM,
            "click to skip",
            FontId::monospace(10.0),
            alpha(theme::TEXT_DIM, 1.0),
        );

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_stays_in_range_and_does_not_stick() {
        let mut rng = Rng::new(7);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..500 {
            let v = rng.next_f32();
            assert!((0.0..1.0).contains(&v), "out of range: {v}");
            seen.insert(v.to_bits());
        }
        assert!(seen.len() > 400, "generator is degenerate: {}", seen.len());
        assert!((0..64).all(|_| rng.below(4) < 4));
    }

    /// Seeded with zero, a plain xorshift stays at zero forever -- which would
    /// pin every rain column to the same speed and glyph.
    #[test]
    fn rng_survives_a_zero_seed() {
        let mut rng = Rng::new(0);
        assert_ne!(rng.next_u64(), 0);
        assert_ne!(rng.next_u64(), rng.next_u64());
    }

    #[test]
    fn cursor_blinks_both_ways() {
        assert!(cursor_on(0.0));
        assert!(!cursor_on(0.9));
    }

    /// The wobble has to stay invisible-as-a-pulse: a flicker that dips more
    /// than a few percent reads as a broken display, not as a CRT.
    #[test]
    fn flicker_stays_subtle() {
        for step in 0..2000 {
            let f = flicker(step as f64 * 0.017);
            assert!((0.97..=1.03).contains(&f), "flicker spiked to {f}");
        }
    }

    /// The boot cover must be short. It is in front of the start button, so
    /// its length is dead time for the user every single launch.
    #[test]
    fn boot_sequence_is_under_two_seconds() {
        let total = BootSequence::total_chars() / BOOT_CPS + BOOT_HOLD + BOOT_FADE;
        assert!(total < 2.0, "boot cover lasts {total}s");
    }

    /// Every rain glyph is ASCII -- the bundled font guarantees nothing else.
    #[test]
    fn rain_glyphs_are_ascii() {
        assert!(RAIN_GLYPHS.iter().all(|b| b.is_ascii_graphic()));
    }

    /// Same rule for the boot log, which is the first thing ever drawn.
    #[test]
    fn boot_lines_are_ascii() {
        for line in BOOT_LINES {
            assert!(line.text.is_ascii(), "non-ascii boot line: {}", line.text);
        }
    }
}
