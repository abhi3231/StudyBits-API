import logging
from flask import Blueprint, request, jsonify
from typing import List, Dict, Set, Optional
from pyparsing import Iterable
from firebase.firebase import db
import random
import re

question_match = Blueprint("question_match", __name__)
STOPWORDS = {"the", "and", "or", "of", "a", "an", "in", "on", "to", "for", "by", "with", "at", "from", "as", "is"}

logging.basicConfig(level=logging.DEBUG)

class QuestionMatcher:
    def get_user_learning_state(self, uid: str, course: str) -> Optional[dict]:
        doc = db.collection("learning").document(uid).collection("courses").document(course).get()
        return doc.to_dict() if doc.exists else None

    def split_tags(self, tags: Iterable[str]) -> Set[str]:
        words = set()
        for tag in tags:
            tag = tag.lower()
            tag = re.sub(r"[-/]", " ", tag)
            tag = re.sub(r"[^\w\s]", "", tag)
            for word in tag.split():
                if word in STOPWORDS or not word.strip():
                    continue
                if word.endswith("s") and len(word) > 3:
                    word = word[:-1]
                words.add(word)
        return words

    def get_question_tags(self, question_ids: List[str]) -> Set[str]:
        tags = set()
        for qid in question_ids:
            doc = db.collection("questions").document(qid).get()
            if doc.exists:
                raw_tags = doc.to_dict().get("tags", [])
                split = self.split_tags(raw_tags)
                logging.info("2. Tags for question '%s': %s", qid, split)
                tags.update(split)
        return tags

    def get_course_tags(self, course_id: str) -> Set[str]:
        doc = db.collection("courses").document(course_id).get()
        if doc.exists:
            raw_tags = doc.to_dict().get("tags", [])
            tags = self.split_tags(raw_tags)
            logging.info("3. Tags for course '%s': %s", course_id, tags)
            return tags
        return set()

    def get_unit_tags(self, course_id: str, unit_id: Optional[str]) -> Set[str]:
        if not unit_id:
            return set()
        doc = (
            db.collection("courses")
            .document(course_id)
            .collection("units")
            .document(unit_id)
            .get()
        )
        if doc.exists:
            raw_tags = doc.to_dict().get("tags", [])
            tags = self.split_tags(raw_tags)
            logging.info("4. Tags for unit '%s' in course '%s': %s", unit_id, course_id, tags)
            return tags
        return set()

    def get_effective_tags(self, question: dict) -> Set[str]:
        qtags = self.split_tags(question.get("tags", []))
        course_id = question.get("course")
        unit_id = question.get("unit")
        effective = qtags | self.get_course_tags(course_id) | self.get_unit_tags(course_id, unit_id)
        logging.debug("5. Effective tags for question '%s': %s", question.get("id", "unknown"), effective)
        return effective

    def find_relevant_questions(
    self,
    liked_tags: Set[str],
    disliked_tags: Set[str],
    course_tags: Set[str],
    unit_tags: Set[str],
    answered_questions: Set[str],
    subscribed_courses: Set[str],
    match_threshold: float = 0.5,
    disliked_threshold: float = 0.4,
    reference_course_id: Optional[str] = None, 
    reference_unit_id: Optional[str] = None  
) -> List[Dict]:
        matched = []
        curriculum_tags = course_tags | unit_tags
        logging.info("6. Curriculum tags: %s", curriculum_tags)

        for doc in db.collection("questions").stream():
            qdata = doc.to_dict()
            qid = doc.id
            qdata["id"] = qid

            effective_tags = self.get_effective_tags(qdata)
            if not effective_tags:
                logging.debug("7. Skipping question '%s': no effective tags", qid)
                continue

            disallowed_disliked_tags = effective_tags & disliked_tags - curriculum_tags
            dislike_ratio = len(disallowed_disliked_tags) / len(effective_tags)
            if dislike_ratio > disliked_threshold:
                logging.debug("8. Skipping question '%s': too many disliked tags (%.2f)", qid, dislike_ratio)
                continue

            num_required = len(curriculum_tags)
            if num_required == 0:
                continue

            num_matched = len(effective_tags & curriculum_tags)
            match_ratio = num_matched / num_required
            if match_ratio < match_threshold:
                logging.debug("9. Skipping question '%s': curriculum match ratio too low (%.2f)", qid, match_ratio)
                continue

            liked_overlap = len(effective_tags & liked_tags)
            liked_ratio = liked_overlap / len(effective_tags)
            liked_boost = round(liked_ratio * 2, 2)

            course_id = qdata.get("course")
            unit_id = qdata.get("unit")

            subscribed_boost = 1 if course_id in subscribed_courses else 0
            answered_penalty = -1 if qid in answered_questions else 0
            same_course_boost = 1 if course_id == reference_course_id else 0
            same_unit_boost = 1 if reference_unit_id and unit_id == reference_unit_id else 0

            priority = liked_boost + subscribed_boost + same_course_boost + same_unit_boost + answered_penalty

            logging.info(
                "10. Matched question '%s': liked=%d, match_ratio=%.2f, dislike_ratio=%.2f, priority=%.2f",
                qid, liked_overlap, match_ratio, dislike_ratio, priority
            )

            matched.append({
                "course_id": course_id,
                "course_name": qdata.get("course_name", ""),
                "unit_id": unit_id,
                "unit_name": qdata.get("unit_name", ""),
                "question_id": qid,
                "score": liked_overlap,
                "priority": priority
            })

        return matched

    def group_and_rank(self, matched: List[Dict], top_k: int) -> List[Dict]:
        grouped = {}

        for item in matched:
            key = (item["course_id"], item["unit_id"])
            if key not in grouped:
                grouped[key] = {
                    "course_id": item["course_id"],
                    "unit_id": item["unit_id"],
                    "questions": [],
                    "priority": 0,
                    "total_score": 0
                }

            grouped[key]["questions"].append(item["question_id"])
            grouped[key]["priority"] += item["priority"]
            grouped[key]["total_score"] += item["score"]

        for (course_id, unit_id), group in grouped.items():
            course_doc = db.collection("courses").document(course_id).get()
            group["course_name"] = course_doc.to_dict().get("name", "") if course_doc.exists else ""

            if unit_id:
                unit_doc = db.collection("courses").document(course_id).collection("units").document(unit_id).get()
                group["unit_name"] = unit_doc.to_dict().get("name", "") if unit_doc.exists else ""
            else:
                group["unit_name"] = ""

        sorted_groups = sorted(
            grouped.values(),
            key=lambda g: (-g["priority"], -g["total_score"])
        )

        logging.info("11. Top %d groups sorted by priority and score", top_k)
        for group in sorted_groups[:top_k]:
            logging.info("   Group: Course '%s', Unit '%s', Priority=%.2f, Score=%d, Questions=%s",
                        group["course_id"], group["unit_id"], group["priority"], group["total_score"], group["questions"])

        top_results = sorted_groups[:top_k]
        random.shuffle(top_results)

        return [
            {
                "course_id": group["course_id"],
                "course_name": group["course_name"],
                "unit_id": group["unit_id"],
                "unit_name": group["unit_name"],
                "questions": group["questions"]
            }
            for group in top_results
        ]

matcher = QuestionMatcher()

@question_match.route("/find_similar_courses", methods=["POST"])
def find_similar_courses():
    data = request.json
    uid = data.get("uid")
    course_id = data.get("course_id")
    unit_id = data.get("unit_id")
    use_units = data.get("useUnits", False)
    top_k = data.get("top_k", 5)

    if not uid or not course_id:
        return jsonify({"error": "Missing required fields"}), 400

    logging.info("=== Starting similarity search for user '%s', course '%s', unit '%s' ===", uid, course_id, unit_id)

    user_data = matcher.get_user_learning_state(uid, course_id)
    if not user_data:
        return jsonify({"error": "User not found"}), 404

    liked_ids = user_data.get("likedQuestions", [])
    disliked_ids = user_data.get("dislikedQuestions", [])
    answered_ids = set(user_data.get("answeredQuestions", []))
    subscribed_courses = set(user_data.get("subscribedCourses", []))

    logging.info("12. Liked Questions: %s", liked_ids)
    logging.info("13. Disliked Questions: %s", disliked_ids)
    logging.info("14. Answered Questions: %s", answered_ids)
    logging.info("15. Subscribed Courses: %s", subscribed_courses)

    liked_tags = matcher.get_question_tags(liked_ids)
    disliked_tags = matcher.get_question_tags(disliked_ids)
    course_tags = matcher.get_course_tags(course_id)
    unit_tags = matcher.get_unit_tags(course_id, unit_id) if use_units and unit_id else set()

    matched = matcher.find_relevant_questions(
        liked_tags=liked_tags,
        disliked_tags=disliked_tags,
        course_tags=course_tags,
        unit_tags=unit_tags,
        answered_questions=answered_ids,
        subscribed_courses=subscribed_courses,
        match_threshold=0.5,
        disliked_threshold=0.4,
        reference_course_id=course_id,
        reference_unit_id=unit_id if use_units else None
    )

    result = matcher.group_and_rank(matched, top_k)
    logging.info("16. Returning %d similar course-unit results", len(result))

    return jsonify({"similar_courses": result}), 200